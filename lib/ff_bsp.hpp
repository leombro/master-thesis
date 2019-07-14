//
// Created by Orlando Leombruni on 2019-06-24.
//

#ifndef FF_BSP
#define FF_BSP

#include <memory>
#include <vector>
#include <functional>
#include <climits>
#include <optional>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <ff/multinode.hpp>

#include <iostream>
#define LOG(x) std::cout << x << std::endl

typedef unsigned node_id;
typedef unsigned sstep_id;

static const sstep_id STOP = static_cast<sstep_id>(UINT_MAX);

static const void* PROCEED = (void*)(ULLONG_MAX-11);

template <typename in_t, typename out_t>
class bsp;

namespace {
    typedef enum {
        BEGIN_SUPERSTEP,
        RECEIVE_DATA,
        FIRST_COMPUTATION,
        FLUSH
    } Task;
}

template <typename A, typename B = A>
using pair = std::pair<A, B>;

typedef enum {
    INTERNAL,
    MULTI,
    SINGLE,
    ANY,
    ALL,
    FLUSH,
    CONTINUE
} send_type;


class bsp_send {
private:
    std::vector<std::shared_ptr<void>> data;
    std::vector<node_id> to;
    send_type type = INTERNAL;
    node_id sender = 0;

    template <typename, typename>
    friend class bsp;


    template <typename T>
    friend bsp_send bsp_multisend(std::vector<T>& data, std::vector<T>& destinations);
    template <typename T>
    friend bsp_send bsp_any_send(T& what);
    template <typename T>
    friend bsp_send bsp_all_send(T& what);
    template <typename T>
    friend bsp_send bsp_return(T& what);

    explicit bsp_send(node_id id): sender{id} {};

    explicit bsp_send(send_type tp): type{tp} {};

public:
    bsp_send() = delete;

    template <typename T>
    bsp_send(T& element, node_id dest) {
        type = SINGLE;
        data.emplace_back(std::make_shared<T>(element));
        to.emplace_back(dest);
    }

    // copy constructor
    bsp_send(const bsp_send& other) = default;

    bsp_send(bsp_send&& other) noexcept: data(std::move(other.data)), to(std::move(other.to)), type(other.type), sender(other.sender) {

    }


};

template <typename T>
bsp_send bsp_multisend(std::vector<T>& data, std::vector<T>& destinations) {
    if (data.size() == 0 || destinations.size() == 0)
        throw std::runtime_error{"Data and destination vectors cannot be empty"};
    if (data.size() != destinations.size())
        throw std::runtime_error{"Data and destination vectors must have same number of elements"};
    bsp_send toRet{MULTI};
    for (const auto& el: data) {
        toRet.data.emplace_back(std::make_shared<T>(el));
    }
    toRet.to = std::move(destinations);
    return toRet;
}

template <typename T>
bsp_send bsp_any_send(T& what) {
    bsp_send toRet{send_type::ANY};
    toRet.data.emplace_back(std::make_shared<T>(what));
    return toRet;
}

template <typename T>
bsp_send bsp_all_send(T& what) {
    bsp_send toRet{send_type::ALL};
    toRet.data.emplace_back(std::make_shared<T>(what));
    return toRet;
}

template <typename T>
bsp_send bsp_return(T& what) {
    bsp_send toRet{send_type::FLUSH};
    toRet.data.emplace_back(std::make_shared<T>(what));
    return toRet;
}

template <typename T1>
using bsp_processor = std::function<bsp_send(T1, node_id)>;

template <typename T>
using bsp_step_selector = std::optional<std::function<sstep_id(T, sstep_id)>>;

template <typename in_t, typename out_t = in_t>
class bsp_superstep {
protected:
    std::vector<bsp_processor<in_t>> functions;
    bsp_step_selector<out_t> selector;

    size_t size;

    template <typename, typename>
    friend class bsp;

public:

    bsp_superstep() = delete;

    explicit bsp_superstep(std::vector<bsp_processor<in_t>>& computation): functions{computation}, selector({}) {
        size = functions.size();
    };

    bsp_superstep(std::vector<bsp_processor<in_t>>& computation, std::function<sstep_id(out_t, sstep_id)> sel):
        functions{computation}, selector{sel} {
        size = functions.size();
    }
};

template <typename in_t, typename out_t = in_t>
class bsp: public ff::ff_node {
private:
    class typeinfo {
    public:
        std::string name;
        bool isVector;

        typeinfo(std::string _name, bool _isVector): name{std::move(_name)}, isVector{_isVector} {};

        bool operator==(const typeinfo& other) {
            return (name == other.name && isVector == other.isVector);
        }

        bool operator!=(const typeinfo& other) {
            return !operator==(other);
        }

        bool backward_compatible(const typeinfo& other) {
            return (name == other.name && isVector && !other.isVector);
        }
    };

    template <typename T>
    struct is_vector: std::false_type {
        std::string name{typeid(T).name()};
        T* get_pointer() {
            return new T;
        }
    };

    template <typename T>
    struct is_vector<std::vector<T>>: std::true_type {
        std::string name{typeid(T).name()};
        std::vector<T>* get_pointer() {
            return new std::vector<T>;
        }
    };

    template <typename T>
    struct is_vector<T[]>: std::true_type {
        std::string name{typeid(T).name()};
        std::vector<T>* get_pointer() {
            return new std::vector<T>;
        }
    };

    using bsp_task = pair<Task, pair<std::shared_ptr<void>, sstep_id>>;

    struct bsp_master: ff::ff_node_t<bsp_send, bsp_task> {

        ff::ff_loadbalancer* lb = nullptr;
        std::vector<in_t> input;
        bsp<in_t, out_t>* super;
        std::shared_ptr<void> last_received;
        sstep_id curr = -1;
        size_t n_nodes;
        std::vector<int> barrier_count;

        std::vector<std::vector<out_t>> output_temp;

        bsp_master(ff::ff_loadbalancer* const loba, std::vector<in_t>& vec, bsp<in_t, out_t>* sup): lb{loba}, input{std::move(vec)}, super{sup}, n_nodes{sup->max_n_processors} {
            output_temp.resize(n_nodes);
            barrier_count.resize(n_nodes, -1);
        }

        std::vector<out_t> get_output() {
            std::vector<out_t> results;
            for (const auto& vec: output_temp) {
                for (const auto& el: vec) {
                    results.emplace_back(el);
                }
            }
            return results;
        }

        bsp_task* svc(bsp_send* in) override {
            if (in == (bsp_send*)EOS) return EOS;
            if (in == nullptr && curr == -1) {
                curr = 0;
                for (size_t i{0}; i < n_nodes; ++i) {
                    auto to_send = new bsp_task;
                    to_send->first = Task::FIRST_COMPUTATION;
                    to_send->second.first = std::make_shared<in_t>(input[i]);
                    to_send->second.second = curr;
                    barrier_count[i] = 1;
                    lb->ff_send_out_to(to_send, i);
                }
            }  else {
                if (in) {
                    switch (in->type) {
                        case send_type::INTERNAL: {
                            break;
                        }
                        case send_type::CONTINUE: {
                            barrier_count[in->sender] -= 1;
                            if (barrier_count[in->sender] < 0) throw std::runtime_error{"Incorrect number of ACK received from " + std::to_string(in->sender)};
                            if (std::all_of(barrier_count.begin(), barrier_count.end(), [](int i){return i == 0;})) {
                                auto prev = curr;
                                curr = super->superstep_selectors[curr](last_received, curr+1);
                                if (curr == STOP) {
                                    for (size_t i{0}; i < n_nodes; ++i) {
                                        auto to_send = new bsp_task;
                                        to_send->first = Task::FLUSH;
                                        to_send->second.second = curr;
                                        lb->ff_send_out_to(to_send, i);
                                    }
                                } else {
                                    if (curr != prev + 1 && super->superstep_types[prev].second != super->superstep_types[curr].first) {
                                        if (!super->superstep_types[curr].first.backward_compatible(super->superstep_types[prev].second))
                                            throw std::runtime_error{"Consecutive superstep types are not compatible"};
                                    }
                                    for (size_t i{0}; i < n_nodes; ++i) {
                                        auto task = new bsp_task;
                                        barrier_count[i] += 1;
                                        task->first = Task::BEGIN_SUPERSTEP;
                                        task->second.second = curr;
                                        lb->ff_send_out_to(task, i);
                                    }
                                }
                            }
                            break;
                        }
                        case send_type::SINGLE: {
                            barrier_count[in->sender] -= 1;
                            if (barrier_count[in->sender] < 0) throw std::runtime_error{"Incorrect number of ACK received from " + std::to_string(in->sender)};
                            barrier_count[in->to[0]] += 1;
                            auto task = new bsp_task;
                            task->first = Task::RECEIVE_DATA;
                            task->second.first = in->data[0];
                            last_received = in->data[0];
                            task->second.second = curr;
                            lb->ff_send_out_to(task, in->to[0]);
                            break;
                        }
                        case send_type::ANY: {
                            barrier_count[in->sender] -= 1;
                            if (barrier_count[in->sender] < 0) throw std::runtime_error{"Incorrect number of ACK received from " + std::to_string(in->sender)};
                            barrier_count[0] += 1;
                            auto task = new bsp_task;
                            task->first = Task::RECEIVE_DATA;
                            task->second.first = in->data[0];
                            task->second.second = curr;
                            last_received = in->data[0];
                            lb->ff_send_out_to(task, 0);
                            break;
                        }
                        case send_type::ALL: {
                            barrier_count[in->sender] -= 1;
                            if (barrier_count[in->sender] < 0) throw std::runtime_error{"Incorrect number of ACK received from " + std::to_string(in->sender)};
                            last_received = in->data[0];
                            for (size_t i{0}; i < n_nodes; ++i) {
                                barrier_count[i] += 1;
                                auto task = new bsp_task;
                                task->first = Task::RECEIVE_DATA;
                                task->second.first = in->data[0];
                                task->second.second = curr;
                                lb->ff_send_out_to(task, i);
                            }
                            break;
                        }
                        case send_type::MULTI: {
                            barrier_count[in->sender] -= 1;
                            if (barrier_count[in->sender] < 0) throw std::runtime_error{"Incorrect number of ACK received from " + std::to_string(in->sender)};
                            last_received = in->data[0];
                            for (size_t i{0}; i < in->data.size(); ++i) {
                                barrier_count[in->to[i]] += 1;
                                auto task = new bsp_task;
                                task->first = Task::RECEIVE_DATA;
                                task->second.first = in->data[i];
                                task->second.second = curr;
                                lb->ff_send_out_to(task, in->to[i]);
                            }
                            break;
                        }
                        case send_type::FLUSH: {
                            if (in->data[0] != nullptr) {
                                auto vec = reinterpret_cast<std::vector<std::shared_ptr<void>>*>(in->data[0].get());
                                for (const auto& el: *vec) {
                                    auto elem = reinterpret_cast<out_t*>(el.get());
                                    output_temp[in->sender].emplace_back(*elem);
                                }
                            }
                            lb->ff_send_out_to(EOS, in->sender);
                        }
                    }
                    delete in;
                }
            }
            return GO_ON;
        }

    };

    struct bsp_node: ff::ff_node_t<bsp_task, bsp_send> {

        node_id id;
        bsp<in_t, out_t>* super;
        std::vector<std::shared_ptr<void>> stored_msg;

        bsp_node(node_id _id, bsp<in_t, out_t>* _super): id{_id}, super{_super} {
        };

        bsp_send* svc(bsp_task* in) override {
            bsp_send* toRet = nullptr;
            if (in == nullptr) return nullptr;
            switch (in->first) {
                case Task::FIRST_COMPUTATION:
                    if (super->superstep_functions[in->second.second].size() <= id) {
                        toRet = new bsp_send(send_type::CONTINUE);
                        toRet->sender = id;
                    } else {
                        auto fn = super->superstep_functions[in->second.second][id];
                        toRet = new bsp_send(std::move(fn(in->second.first, id)));
                        toRet->sender = id;
                    }
                    break;
                case Task::RECEIVE_DATA:
                    stored_msg.emplace_back(in->second.first);
                    toRet = new bsp_send(send_type::CONTINUE);
                    toRet->sender = id;
                    break;
                case Task::BEGIN_SUPERSTEP:
                    if (super->superstep_functions[in->second.second].size() <= id) {
                        toRet = new bsp_send(send_type::CONTINUE);
                        toRet->sender = id;
                    } else {
                        auto fn = super->superstep_functions[in->second.second][id];
                        if (stored_msg.size() == 1) {
                            toRet = new bsp_send(std::move(fn(stored_msg.at(0), id)));
                        } else {
                            std::shared_ptr<void> mes = super->superstep_transform_accumulator[in->second.second](stored_msg);
                            toRet = new bsp_send(std::move(fn(mes, id)));
                        }
                        toRet->sender = id;
                        stored_msg.clear();
                    }
                    break;
                case Task::FLUSH: {
                    toRet = new bsp_send(stored_msg, 0);
                    toRet->sender = id;
                    toRet->type = send_type::FLUSH;
                    break;
                }
            }
            delete in;
            return toRet;
        }

    };

    std::vector<std::vector<bsp_processor<std::shared_ptr<void>>>> superstep_functions;
    std::vector<std::function<sstep_id(std::shared_ptr<void>, sstep_id)>> superstep_selectors;
    std::vector<std::function<std::shared_ptr<void>(std::vector<std::shared_ptr<void>>&)>> superstep_transform_accumulator;
    std::vector<pair<typeinfo>> superstep_types;

    std::vector<in_t> input_data;
    unsigned n_supersteps{0};
    unsigned max_n_processors{0};
    bool initialized{false};
    bool last_sstep_default_selector{true};
    std::string last_typename{typeid(in_t).name()};

    bool compare_types_util(const std::string& a, const std::string& b, std::true_type) {
        return a == b;
    }

    bool compare_types_util(const std::string&, const std::string&, std::false_type) {
        return false;
    }

    template <typename T>
    bool is_vector_type(const std::string& type_name) {
        is_vector<T> m{};
        return compare_types_util(type_name, m.name, m);
    }

    template <typename T>
    typeinfo get_typeinfo() {
        is_vector<T> m{};
        return {m.name, m.value};
    }

    template <typename T1, typename T2>
    sstep_id add_superstep_common(const bsp_superstep<T1, T2>& sstep) {
        if (sstep.size == 0) {
            throw std::runtime_error{"Empty superstep"};
        }
        if (initialized) {
            throw std::runtime_error{"Process was already finalized"};
        }
        if (n_supersteps == 0 && input_data.size() > 0 && (sstep.functions.size() != input_data.size())) {
            throw std::runtime_error{"First superstep's processors must be of the same cardinality as input data"};
        }
        if (last_sstep_default_selector) {
            if (typeid(T1).name() != last_typename && !is_vector_type<T1>(last_typename)) {
                throw std::runtime_error{"Superstep input type must be the same as previous superstep's output type"};
            }
        }
        last_typename = typeid(T2).name();
        superstep_types.emplace_back(pair<typeinfo>{get_typeinfo<T1>(), get_typeinfo<T2>()});
        std::vector<bsp_processor<std::shared_ptr<void>>> functions;
        for (const auto& fun: sstep.functions) {
            functions.emplace_back([&fun](const std::shared_ptr<void>& in, node_id id) -> bsp_send {
                auto in_ptr = reinterpret_cast<T1*>(in.get());
                return fun(*in_ptr, id);
            });
        }
        superstep_functions.emplace_back(functions);
        if (sstep.selector.has_value()) {
            last_sstep_default_selector = false;
            superstep_selectors.emplace_back([&f = sstep.selector.value()](const std::shared_ptr<void>& in, sstep_id id) -> sstep_id {
                auto in_ptr = reinterpret_cast<T2*>(in.get());
                sstep_id ret = f(*in_ptr, id);
                if (ret == STOP) return STOP;
                return (ret - 1);
            });
        } else {
            last_sstep_default_selector = true;
            superstep_selectors.emplace_back([](const std::shared_ptr<void>&, sstep_id id) -> sstep_id {
                return id;
            });
        }
        if (sstep.size > max_n_processors) {
            max_n_processors = sstep.size;
        }
        return ++n_supersteps;
    }

    template <typename T1, typename T2>
    sstep_id add_superstep_internal (const bsp_superstep<T1, T2>& sstep, std::false_type) {
        superstep_transform_accumulator.emplace_back([](const std::vector<std::shared_ptr<void>>& accumulator) -> std::shared_ptr<void> {
            return std::make_shared<std::vector<std::shared_ptr<void>>>(accumulator);
        });

        return add_superstep_common(sstep);
    }

    template <typename T1, typename T2>
    sstep_id add_superstep_internal(const bsp_superstep<T1, T2>& sstep, std::true_type) {
        superstep_transform_accumulator.emplace_back([](const std::vector<std::shared_ptr<void>>& accumulator) -> std::shared_ptr<void> {
            typedef typename T1::value_type eltype;
            auto vec = new T1;
            for (const auto &el: accumulator) {
                auto t1ptr = reinterpret_cast<eltype*>(el.get());
                vec->emplace_back(*t1ptr);
            }
            std::shared_ptr<void> toRet = std::shared_ptr<T1>(vec);
            return toRet;
        });

        return add_superstep_common(sstep);
    }

public:

    bsp() = default;

    explicit bsp(std::vector<in_t>& input): input_data{input} {};

    void add_input(std::vector<in_t>& input) {
        if (input_data.size() > 0) {
            throw std::runtime_error{"Input data already provided"};
        }
        if (n_supersteps > 0 && superstep_functions[0].size() != input.size()) {
            throw std::runtime_error{"Input data must be of the same cardinality as first superstep's processors"};
        }
        input_data = std::move(input);
    }

    template <typename T1, typename T2>
    sstep_id add_superstep(const bsp_superstep<T1, T2>& sstep) {
        is_vector<T1> m{};
        return add_superstep_internal(sstep, m);
    }

    void finalize() {
        if (initialized) throw std::runtime_error{"Process was already finalized"};
        if (n_supersteps == 0) throw std::runtime_error{"Must include at least one superstep"};
        if (input_data.empty()) throw std::runtime_error{"Input data is empty"};
        if (last_typename != typeid(out_t).name())
            throw std::runtime_error{"Last superstep output type doesn't match BSP output type"};
        initialized = true;
    }

    std::vector<out_t> run() {
        if (!initialized) {
            if (!initialized) throw std::runtime_error{"BSP structure not initialized"};
        }
        std::vector<std::unique_ptr<ff::ff_node>> workers;
        for (size_t i{0}; i < max_n_processors; ++i) {
            workers.emplace_back(std::make_unique<bsp_node>(i, this));
        }
        ff::ff_Farm<> farm(std::move(workers));
        bsp_master master{farm.getlb(), input_data, this};
        farm.add_emitter(master);
        farm.wrap_around();
        farm.run_and_wait_end();
        return master.get_output();
    }

    void* svc(void* task) override {
        return task;
    }
};

#endif //FF_BSP
