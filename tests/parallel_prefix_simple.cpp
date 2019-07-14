//
// Created by Orlando Leombruni on 2019-06-15.
//

#include "ff_bsp.hpp"
#include <iostream>
#include <sstream>

template <typename T>
void printvect(std::vector<T> v) {
    std::cout << "[ ";
    for (const auto& i: v) {
        std::cout << i << " ";
    }
    std::cout << "]" << std::endl;
}

template <typename T>
void printvvect(std::vector<std::vector<T>> vv) {
    std::cout << "[" << std::endl;
    for (const auto& v: vv) {
        std::cout << "\t[ ";
        for (const auto& i: v) {
            std::cout << i << " ";
        }
        std::cout << "]" << std::endl;
    }
    std::cout << "]" << std::endl;
}

int main() {

    std::vector<std::vector<int>> input(4);
    static std::vector<int> intern(25);
    std::fill(intern.begin(), intern.end(), 1);
    printvect(intern);
    std::fill(input.begin(), input.end(), intern);

    printvvect(input);

    std::vector<bsp_processor<std::vector<int>>> phase1funcs(4);
    std::fill(phase1funcs.begin(), phase1funcs.end(), [](std::vector<int> in, node_id node) -> bsp_send {
        int count = 0;
        std::ostringstream oss;
        oss << "node " << node << " received: [ ";
        for (const auto& el: in) {
            oss << el << " ";
            count += el;
        }
        oss << "]";
        std::cout << oss.str() << std::endl;
        return bsp_any_send(count);
    });

    std::vector<bsp_processor<std::vector<int>>> phase2func(1);
    phase2func[0] = [](std::vector<int> in, node_id node) -> bsp_send {
        int acc = 0;
        std::cout << "middle step [ ";
        for (auto& el: in) {
            int temp = el;
            el = acc;
            acc += temp;
            std::cout << el << " ";
        }
        std::cout << "]" << std::endl;
        auto toRet = std::pair{in, intern};
        return bsp_all_send(toRet);
    };

    std::vector<bsp_processor<std::pair<std::vector<int>, std::vector<int>>>> phase3funcs(4);
    std::fill(phase3funcs.begin(), phase3funcs.end(), [](std::pair<std::vector<int>, std::vector<int>> in, node_id node) -> bsp_send {
        int myReduc = in.first[node];
        std::ostringstream oss;
        int acc = myReduc;
        oss << "node " << node << " third step: [ ";
        for (auto& el: in.second) {
            int temp = el;
            el = acc;
            acc += temp;
            oss << el << " ";
        }
        oss << "]";
        std::cout << oss.str() << std::endl;
        return bsp_send(in.second, node);
    });

    bsp_superstep<std::vector<int>, int> step1(phase1funcs);
    bsp_superstep<std::vector<int>, std::pair<std::vector<int>, std::vector<int>>> step2(phase2func);
    bsp_superstep<std::pair<std::vector<int>, std::vector<int>>, std::vector<int>> step3(phase3funcs, [](auto, sstep_id) {return STOP;});

    bsp<std::vector<int>> bspfun(input);
    bspfun.add_superstep(step1);
    bspfun.add_superstep(step2);
    bspfun.add_superstep(step3);

    bspfun.finalize();

    printvvect(bspfun.run());
}

