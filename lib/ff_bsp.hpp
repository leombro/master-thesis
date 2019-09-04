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
#include <mutex>
#include <condition_variable>
#include <ff/ff.hpp>

#include <iostream>
#define LOG(x) std::cout << x << std::endl

typedef unsigned node_id;
typedef unsigned sstep_id;

static const sstep_id STOP = static_cast<sstep_id>(UINT_MAX);

static const void* PROCEED = (void*)(ULLONG_MAX-11);

using bsp_time = std::chrono::time_point<std::chrono::high_resolution_clock>;

namespace {
    typedef enum {
        BEGIN_SUPERSTEP,
        BEGIN_SUPERSTEP_AGGR,
        RECEIVE_DATA,
        RECEIVE_DATA_AGGR,
        FIRST_COMPUTATION,
        FLUSH
    } Task;

    class barrier {
    public:

        barrier() = default;

        barrier(const barrier& other):
            mThreshold{other.mThreshold},
            mCount{other.mCount},
            mGeneration{other.mGeneration}{
        }

        void setBarrier(std::size_t iCount) {
            mThreshold = iCount;
            mCount = iCount;
            mGeneration = 0;
        }

        void wait() {
            std::unique_lock<std::mutex> lLock{mMutex};
            auto lGen = mGeneration;
            if (!--mCount) {
                mGeneration++;
                mCount = mThreshold;
                mCond.notify_all();
            } else {
                mCond.wait(lLock, [this, lGen] { return lGen != mGeneration; });
            }
        }

    private:
        std::mutex mMutex;
        std::condition_variable mCond;
        std::size_t mThreshold = 0;
        std::size_t mCount = 0;
        std::size_t mGeneration = 0;
    };
}

template <typename A, typename B = A>
using pair = std::pair<A, B>;


class bsp_send {
protected:
    std::vector<std::shared_ptr<void>> data;
    std::vector<node_id> to;
    std::vector<bool> broadcast;
    int size = 0;

    template <typename, typename>
    friend class bsp;


public:
    bsp_send() = default;

    template <typename T>
    bsp_send(T& element, node_id dest, bool to_broadcast = false) {
        data.emplace_back(std::make_shared<T>(element));
        to.emplace_back(dest);
        broadcast.emplace_back(to_broadcast);
        size++;
    }

    // copy constructor
    bsp_send(const bsp_send& other) = default;

    bsp_send(bsp_send&& other) noexcept: data(std::move(other.data)), to(std::move(other.to)), broadcast(std::move(other.broadcast)), size(other.size) {

    }

    template <typename T>
    void add(T& element, node_id dest, bool to_broadcast = false) {
        data.emplace_back(std::make_shared<T>(element));
        to.emplace_back(dest);
        broadcast.emplace_back(to_broadcast);
        size++;
    }

};

template <typename T>
using bsp_function = std::function<bsp_send(T, node_id)>;

template <typename T>
using bsp_step_selector = std::optional<std::function<sstep_id(T, sstep_id)>>;

template <typename in_t, typename out_t = in_t>
class bsp_superstep {
protected:
    std::vector<bsp_function<in_t>> functions;
    bsp_step_selector<out_t> selector;
    bsp_time* timeptr = nullptr;

    size_t size;

    template <typename, typename>
    friend class bsp;

public:

    bsp_superstep() = delete;

    explicit bsp_superstep(std::vector<bsp_function<in_t>>& computation, bsp_time* time = nullptr): functions{computation}, selector({}) {
        size = functions.size();
        if (size == 0) throw std::runtime_error("Number of processors in the superstep cannot be zero");
    };

    bsp_superstep(std::vector<bsp_function<in_t>>& computation, std::function<sstep_id(out_t, sstep_id)> sel, bsp_time* time = nullptr):
        functions{computation}, selector{sel} {
        size = functions.size();
        if (size == 0) throw std::runtime_error("Number of processors in the superstep cannot be zero");
    }
};

using bsp_task = std::pair<Task, sstep_id>;

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

    struct bsp_master;

    struct bsp_node: ff::ff_node_t<bsp_task, node_id> {

        node_id id;
        bsp<in_t, out_t>* super;
        size_t nproc = 0;
        size_t count = 0;
        bool aggregate = false;
        std::vector<std::shared_ptr<void>>* receive_memory;
        std::vector<std::shared_ptr<void>>* swap = nullptr;
        bsp_master* coordinator;

        bsp_node(node_id _id, bsp<in_t, out_t>* _super): id{_id}, super{_super}, nproc{_super->max_n_processors} {
            receive_memory = new std::vector<std::shared_ptr<void>>[nproc];
        };

        ~bsp_node() override {
            if (swap != nullptr) delete[] swap;
            delete[] receive_memory;
        }

        void set_coordinator(bsp_master* coord) {
            coordinator = coord;
        }

        bool put(std::shared_ptr<void> what, node_id from) {
            receive_memory[from].emplace_back(what);
            count++;
            if (count > 1) aggregate = true;
            return aggregate;
        }

        // TODO: garantisci aggregazione

        node_id* svc(bsp_task* in) override {
            if (in == nullptr) throw std::runtime_error("Incorrect nullptr received");
            switch (in->first) {
                case Task::FIRST_COMPUTATION:
                    if (super->superstep_functions[in->second].size() > id) {
                        swap = receive_memory;
                        receive_memory = new std::vector<std::shared_ptr<void>>[nproc];
                        auto fn = super->superstep_functions[in->second][id];
                        auto returned = fn(swap[0][0], id);
                        count = 0;
                        aggregate = false;
                        coordinator->request_write();
                        for (int i{0}; i < returned.size; ++i) {
                            auto what = returned.data[i];
                            if (returned.broadcast[i]) {
                                coordinator->put_all(what, id);
                            } else {
                                auto dest = returned.to[i];
                                coordinator->put(what, id, dest);
                            }
                        }
                        delete[] swap;
                        swap = nullptr;
                    } else {
                        coordinator->request_write();
                    }
                    break;
                case Task::BEGIN_SUPERSTEP_AGGR:
                    aggregate = true;
                case Task::BEGIN_SUPERSTEP:
                    swap = receive_memory;
                    receive_memory = new std::vector<std::shared_ptr<void>>[nproc];
                    if (super->superstep_functions[in->second].size() > id) {
                        auto fn = super->superstep_functions[in->second][id];
                        bsp_send* returned = nullptr;
                        if (aggregate) {
                            std::vector<std::shared_ptr<void>> ordered;
                            for (size_t i{0}; i < nproc; ++i) {
                                for (auto el: swap[i]) {
                                    ordered.emplace_back(el);
                                }
                            }
                            auto msg = super->superstep_transform_accumulator[in->second](ordered);
                            returned = new bsp_send(std::move(fn(msg, id)));
                        } else {
                            for (size_t i{0}; i < nproc && returned == nullptr; ++i) {
                                std::string e = std::to_string(swap[i].size());
                                if (swap[i].size() == 1) {
                                    returned = new bsp_send(std::move(fn(swap[i][0], id)));
                                }
                            }
                            if (returned == nullptr) throw std::runtime_error("Incorrect calculation");
                        }
                        count = 0;
                        aggregate = false;
                        coordinator->request_write();
                        for (int i{0}; i < returned->size; ++i) {
                            std::shared_ptr<void> what = returned->data[i];
                            if (returned->broadcast[i]) {
                                coordinator->put_all(what, id);
                            } else {
                                auto dest = returned->to[i];
                                coordinator->put(what, id, dest);
                            }
                        }
                        delete returned;
                    } else {
                        count = 0;
                        aggregate = false;
                        coordinator->request_write();
                    }
                    delete[] swap;
                    swap = nullptr;
                    break;
                case Task::FLUSH:
                    swap = receive_memory;
                    count = 0;
                    receive_memory = nullptr;
                    for (size_t i{0}; i < nproc; ++i) {
                        for (const auto& el: swap[i]) {
                            coordinator->put_output(el, id);
                        }
                    }
                    delete[] swap;
                    return EOS;
                default:
                    throw std::runtime_error("Unknown task type");
            }
            delete in;
            return new node_id(id);
        }

        /*
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
                case Task::RECEIVE_DATA_AGGR:
                    aggregate = true;
                case Task::RECEIVE_DATA:
                    stored_msg.emplace_back(in->second.first);
                    nodes.emplace_back(std::pair(in->second.second, count++));
                    toRet = new bsp_send(send_type::CONTINUE);
                    toRet->sender = id;
                    toRet->additional_data = (stored_msg.size() > 1);
                    break;
                case Task::BEGIN_SUPERSTEP:
                    if (super->superstep_functions[in->second.second].size() <= id) {
                        toRet = new bsp_send(send_type::CONTINUE);
                        toRet->sender = id;
                    } else {
                        auto fn = super->superstep_functions[in->second.second][id];
                        if (!aggregate) aggregate = in->must_aggregate;
                        if (stored_msg.size() == 1 && !aggregate) {
                            toRet = new bsp_send(std::move(fn(stored_msg.at(0), id)));
                        } else {
                            std::sort(nodes.begin(), nodes.end(), [](const pair<node_id, size_t>& i, const pair<node_id, size_t>& j){return (i.first < j.first);});
                            std::vector<std::shared_ptr<void>> ordered;
                            for (const auto& el: nodes) {
                                ordered.emplace_back(stored_msg.at(el.second));
                            }
                            std::shared_ptr<void> mes = super->superstep_transform_accumulator[in->second.second](ordered);
                            toRet = new bsp_send(std::move(fn(mes, id)));
                        }
                        toRet->sender = id;
                        stored_msg.clear();
                        nodes.clear();
                        count = 0;
                        aggregate = false;
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
        */

    };

    struct bsp_master: ff::ff_node_t<node_id, bsp_task> {

        ff::ff_loadbalancer* lb = nullptr;
        std::vector<in_t> input;
        bsp<in_t, out_t>* super;
        std::shared_ptr<void> last_received;
        sstep_id curr = -1;
        size_t n_nodes;
        std::vector<int> barrier_count;
        std::vector<bsp_node*> workers;
        bool stopping = false;
        ::barrier write_barrier;

        std::vector<std::vector<out_t>> output_temp;

        bsp_master(ff::ff_loadbalancer* const loba,
                std::vector<in_t>& vec,
                bsp<in_t, out_t>* sup,
                const std::vector<bsp_node*>& _workers):
                    lb{loba},
                    input{std::move(vec)},
                    super{sup},
                    n_nodes{sup->max_n_processors},
                    workers{_workers} {
            write_barrier.setBarrier(n_nodes);
            output_temp.resize(n_nodes);
            barrier_count.resize(n_nodes, -1);
            for (const auto& worker: workers) {
                worker->set_coordinator(this);
            }
        }

        void request_write() {
            write_barrier.wait();
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

        void put_output(const std::shared_ptr<void>& what, node_id from) {
            auto element = reinterpret_cast<out_t*>(what.get());
            output_temp.at(from).emplace_back(*element);
        }

        void put(std::shared_ptr<void> what, node_id from, node_id to) {
            workers[to]->put(what, from);
            last_received = std::shared_ptr<void>(what);
        }

        void put_all(std::shared_ptr<void> what, node_id from) {
            for (const auto& worker: workers) {
                worker->put(what, from);
            }
            last_received = std::shared_ptr<void>(what);
        }

        bsp_task* svc(node_id* in) override {
            if (in == nullptr && curr == -1) {
                curr = 0;
                for (size_t i{0}; i < n_nodes; ++i) {
                    auto to_send = new bsp_task;
                    to_send->first = Task::FIRST_COMPUTATION;
                    to_send->second = curr;
                    workers.at(i)->put(std::make_shared<in_t>(input[i]), 0);
                    barrier_count[i] = 1;
                    lb->ff_send_out_to(to_send, i);
                }
            } else {
                if (in) {
                    barrier_count[*in] -= 1;
                    if (std::all_of(barrier_count.begin(), barrier_count.end(), [](int i){return i == 0;})) {
                        if (stopping) {
                            return EOS;
                        }
                        auto prev = curr;
                        curr = super->superstep_selectors[curr](last_received, curr+1);
                        if (curr == STOP) {
                            if (super->superstep_times[prev] != nullptr) {
                                *(super->superstep_times[prev]) = std::chrono::high_resolution_clock::now();
                            }
                            for (size_t i{0}; i < n_nodes; ++i) {
                                auto to_send = new bsp_task;
                                to_send->first = Task::FLUSH;
                                to_send->second = curr;
                                lb->ff_send_out_to(to_send, i);
                            }
                            stopping = true;
                        } else {
                            if (curr != prev + 1 && super->superstep_types[prev].second != super->superstep_types[curr].first) {
                                if (!super->superstep_types[curr].first.backward_compatible(super->superstep_types[prev].second)) {
                                    throw std::runtime_error{"Consecutive superstep types are not compatible"};
                                }
                            }
                            if (curr != prev) {
                                if (super->superstep_times[prev] != nullptr) {
                                    *(super->superstep_times[prev]) = std::chrono::high_resolution_clock::now();
                                }
                            }
                            bool must_aggregate = false;
                            for (size_t i{0}; i < workers.size() && !must_aggregate; ++i) {
                                must_aggregate = workers[i]->aggregate;
                            }
                            for (size_t i{0}; i < n_nodes; ++i) {
                                barrier_count[i] += 1;
                                auto to_send = new bsp_task;
                                to_send->first = (must_aggregate ? Task::BEGIN_SUPERSTEP_AGGR : Task::BEGIN_SUPERSTEP);
                                to_send->second = curr;
                                lb->ff_send_out_to(to_send, i);
                            }
                        }
                    }
                }
                delete in;
            }
            return GO_ON;
        }

        /*bsp_task* svc(bsp_send* in) override {
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
                            if (!aggregate) aggregate = in->additional_data;
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
                                        task->must_aggregate = aggregate;
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
                            task->second.second = in->sender;
                            lb->ff_send_out_to(task, in->to[0]);
                            break;
                        }
                        case send_type::ANY: {
                            barrier_count[in->sender] -= 1;
                            if (barrier_count[in->sender] < 0) throw std::runtime_error{"Incorrect number of ACK received from " + std::to_string(in->sender)};
                            barrier_count[0] += 1;
                            auto task = new bsp_task;
                            task->first = Task::RECEIVE_DATA_AGGR;
                            task->second.first = in->data[0];
                            task->second.second = in->sender;
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
                                task->second.second = in->sender;
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
                                task->second.second = in->sender;
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
        */

    };

    std::vector<std::vector<bsp_function<std::shared_ptr<void>>>> superstep_functions;
    std::vector<std::function<sstep_id(std::shared_ptr<void>, sstep_id)>> superstep_selectors;
    std::vector<std::function<std::shared_ptr<void>(std::vector<std::shared_ptr<void>>&)>> superstep_transform_accumulator;
    std::vector<bsp_time*> superstep_times;
    std::vector<pair<typeinfo>> superstep_types;

    std::vector<in_t> input_data;
    unsigned n_supersteps{0};
    unsigned max_n_processors{0};
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
        superstep_times.emplace_back(sstep.timeptr);
        std::vector<bsp_function<std::shared_ptr<void>>> functions;
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

    std::vector<out_t> run() {
        if (n_supersteps == 0) throw std::runtime_error{"Must include at least one superstep"};
        if (input_data.empty()) throw std::runtime_error{"Input data is empty"};
        if (last_typename != typeid(out_t).name()) {
            throw std::runtime_error{"Last superstep output type doesn't match BSP output type"};
        }
        std::vector<bsp_node*> workers_node;
        std::vector<ff::ff_node*> workers_ff;
        for (size_t i{0}; i < max_n_processors; ++i) {
            auto t = new bsp_node(i, this);
            workers_node.push_back(t);
            workers_ff.push_back(t);
        }
        ff::ff_farm farm(workers_ff);
        bsp_master master{farm.getlb(), input_data, this, workers_node};
        farm.add_emitter(&master);
        farm.wrap_around();
        farm.run_and_wait_end();
        return master.get_output();
    }

    void* svc(void* task) override {
        return task;
    }
};

#endif //FF_BSP
