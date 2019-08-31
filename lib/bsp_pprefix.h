//
// Created by Orlando Leombruni on 21/08/2019.
//

#ifndef BSP_PPREF_H
#define BSP_PPREF_H

#include <vector>
#include <functional>
#include <chrono>
#include "ff_bsp.hpp"

template <typename T>
class ParallelPrefix {

private:

    std::vector<T> data;                    // the input array on which to perform the scan operation
    bool setData{false};                    // whether the input array has been provided
    std::function<T(T,T)> oplus;            // operator for the scan
    T initial;                              // identity value for the oplus operator
    bool setFun{false};                     // whether the operator and identity have been provided
    int pardegree{-1};                      // desired parallelism degree: 0 for "pure" sequential, -1 if not set
    bool inclusive;                         // whether the scan should be inclusive or exclusive
    bool setUp{false};                      // whether all needed parameters have been provided

    std::vector<T> partialResults;          // Vector of results from the first phase

    /**
     * Pure sequential scan, to be performed in case the user requests so (pardegree = 0).
     */
    void sequentialScan() {
        if (setFun && setData) {
            if (inclusive) {
                for (size_t i{0}; i < data.size(); ++i) {
                    initial = oplus(initial, data.at(i));
                    data.at(i) = initial;
                }
            } else {
                size_t sz = data.size();
                if (sz > 0) {
                    for (size_t i{0}; i < sz - 1; ++i) {
                        T temp = data.at(i);
                        data.at(i) = initial;
                        initial = oplus(initial, temp);
                    }
                    data.at(sz - 1) = initial;
                }
            }
        }
    }

public:

    /**
     * Basic constructor for the class. The user only states whether the scan should be exclusive or inclusive,
     * and leaves the definition of other parameters for a later time.
     *
     * @param _inclusive        whether the scan should be inclusive or exclusive
     */
    explicit ParallelPrefix(bool _inclusive):
            inclusive(_inclusive) {
    }

    /**
     * A constructor to specify both the parallelism degree and the type of desired scan. Other parameters must be
     * provided at a later time.
     *
     * @param _inclusive        whether the scan should be inclusive or exclusive
     * @param _pardegree        the desired parallelism degree
     */
    ParallelPrefix(bool _inclusive, int _pardegree):
            inclusive(_inclusive),
            pardegree(_pardegree) {
    }

    /**
     * A constructor to specify all parameters except the parallelism degree, that must be provided at a later time.
     *
     * @param _data             input array
     * @param _oplus            operator for the scan
     * @param _identity         identity value for the oplus operator
     * @param _inclusive        whether the scan should be inclusive or exclusive
     */
    ParallelPrefix(std::vector<T> _data, std::function<T(T,T)> _oplus, T _identity, bool _inclusive):
            data(std::move(_data)),
            oplus(std::move(_oplus)),
            initial(std::move(_identity)),
            inclusive(_inclusive),
            setData(true),
    setFun(true) {

    }

    /**
     * A constructor to specify all parameters. If all parameters are correct (in particular, a non-negative parallelism
     * degree is required), it leaves the object in the "ready to compute" state, unlike other constructors.
     *
     * @param _data             input array
     * @param _oplus            operator for the scan
     * @param _identity         identity value for the oplus operator
     * @param _inclusive        whether the scan should be inclusive or exclusive
     * @param _pardegree        the desired parallelism degree
     */
    ParallelPrefix(std::vector<T> _data,
                   std::function<T(T,T)> _oplus,
                   T _identity,
                   bool _inclusive,
                   int _pardegree):
            data(std::move(_data)),
            oplus(std::move(_oplus)),
            initial(std::move(_identity)),
            inclusive(_inclusive),
            pardegree(_pardegree) {
        if (pardegree > -1) setUp = true;
    }

    /**
     * Sets the parallelism degree.
     *
     * @param _pardegree        the desired parallelism degree (valid if > -1)
     */
    void setParallelism(int _pardegree) {
        if (_pardegree < 0) return;
        pardegree = _pardegree;
        if (setFun && setData) setUp = true; // All required parameters have been provided
    }

    /**
     * Sets the input data array.
     *
     * @param _data             the input data array
     */
    void setInputVector(std::vector<T> _data) {
        data = std::move(_data);
        setData = true;
        if (setFun && pardegree >= 0) setUp = true; // All required parameters have been provided
    }

    /**
     * Sets the operator and its relative identity value.
     *
     * @param _oplus            operator for the scan
     * @param _identity         identity value for the oplus operator
     */
    void setOperator(std::function<T(T,T)> _oplus, T _identity) {
        oplus = std::move(_oplus);
        initial = std::move(_identity);
        setFun = true;
        if (setData && pardegree >= 0) setUp = true; // All required parameters have been provided
    }

    /**
     *  Computes the scan operation, either in parallel (pardegree > 0) or sequentially (pardegree = 0).
     *
     *  If computing it in parallel, it setups the thread pool as described above, then acts as a coordinator:
     *
     *  - partitioning the input array and distributing tasks for the phase 1
     *  - waiting for all threads to finish phase 1
     *  - performing a sequential exclusive scan on the intermediate results
     *  - distributing tasks for phase 2
     *  - waiting for all threads to finish.
     */
    void compute() {
        if (setUp) {
            if (pardegree == 0) sequentialScan();
            else {

                std::vector<std::vector<T>> input;
                size_t begin = 0;
                size_t split_size = data.size()/pardegree;
                int rest = data.size() % pardegree;
                while (begin < data.size()) {
                    size_t end = std::min(data.size(), begin + split_size + (rest-- > 0 ? 1 : 0));
                    input.emplace_back(data.begin() + begin, data.begin() + end);
                    begin = end;
                }
                std::vector<bsp_function<std::vector<T>>> phase1(pardegree);
                std::fill(phase1.begin(), phase1.end(), [&](std::vector<T> in, node_id node){
                    T acc = initial;
                    for (const auto& el: in) {
                        acc = oplus(acc, el);
                    }
                    auto to_ret = std::pair<std::vector<T>, T>(in, acc);
                    return bsp_any_send(to_ret);
                });

                std::vector<bsp_function<std::vector<std::pair<std::vector<T>, T>>>> phase2(1);
                phase2[0] = [&](std::vector<std::pair<std::vector<T>, T>> in, node_id node) {
                    T acc = initial;
                    for (size_t i{0}; i < in.size() - 1; ++i) {
                        T temp = in[i].second;
                        in[i].second = acc;
                        acc = oplus(acc, temp);
                    }
                    in[in.size() - 1].second = acc;
                    bsp_multisend to_return(in[0], 0);
                    for (size_t i{1}; i < in.size(); ++i) {
                        to_return.add(in[i], i);
                    }
                    return to_return;
                };

                std::vector<bsp_function<std::pair<std::vector<T>, T>>> phase3(pardegree);
                bsp_function<std::pair<std::vector<T>, T>> phase3func;
                if (inclusive) {
                    phase3func = [&](std::pair<std::vector<T>, T> in, node_id node) {
                        for (size_t i{0}; i < in.first.size(); ++i) {
                            in.second = oplus(in.second, in.first[i]);
                            in.first[i] = in.second;
                        }
                        return bsp_send(in.first, node);
                    };
                } else {
                    phase3func = [&](std::pair<std::vector<T>, T> in, node_id node){
                        for (size_t i{0}; i < in.first.size() - 1; ++i) {
                            T temp = in.first[i];
                            in.first[i] = in.second;
                            in.second = oplus(in.second, temp);
                        }
                        in.first[in.first.size() - 1] = in.second;
                        return bsp_send(in.first, node);
                    };
                }
                std::fill(phase3.begin(), phase3.end(), phase3func);

                bsp_superstep<std::vector<T>, std::pair<std::vector<T>, T>> first(phase1);
                bsp_superstep<std::vector<std::pair<std::vector<T>, T>>, std::pair<std::vector<T>, T>> second(phase2);
                bsp_superstep<std::pair<std::vector<T>, T>, std::vector<T>> third(phase3, [](auto, sstep_id){return STOP;});

                bsp<std::vector<T>> bsp_comp(input);
                bsp_comp.add_superstep(first);
                bsp_comp.add_superstep(second);
                bsp_comp.add_superstep(third);

                std::vector<std::vector<T>> out(bsp_comp.run());

                data.clear();
                for (const auto& vect: out) {
                    for (const auto& el: vect) {
                        data.emplace_back(el);
                    }
                }

            }
        }
    }

    std::vector<T> popData() {
        setData = false;
        setUp = false;
        return std::move(data);
    }

};


#endif //BSP_PPREF_H
