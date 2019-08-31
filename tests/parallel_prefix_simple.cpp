//
// Created by Orlando Leombruni on 2019-06-15.
//

#include "ff_bsp.hpp"
#include <iostream>
#include <sstream>
#include <random>

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

    static std::vector<int> a{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25};
    static std::vector<int> b{26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50};
    static std::vector<int> c{51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75};
    static std::vector<int> d{76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100};
    std::vector<std::vector<int>> input{a,b,c,d};
    std::random_device dv;
    std::mt19937 gen(dv());
    std::uniform_int_distribution dist(1, 4);

    printvvect(input);

    std::vector<bsp_function<std::vector<int>>> phase1funcs(4);
    std::fill(phase1funcs.begin(), phase1funcs.end(), [&dist, &gen](std::vector<int> in, node_id node) -> bsp_send {
        int count = 0;
        std::ostringstream oss;
        oss << "node " << node << " received: [ ";
        for (const auto& el: in) {
            oss << el << " ";
            count += el;
        }
        int ntimes = dist(gen) * 1000;
        oss << "] ntimes is " << ntimes << std::endl;
        double startrad = 1080;
        while (ntimes > 0) {
            startrad = std::sin(startrad);
            ntimes--;
        }
        oss << "node " << node << " radians are " << startrad;
        std::cout << oss.str() << std::endl;
        return bsp_any_send(count);
    });

    std::vector<bsp_function<std::vector<int>>> phase2func(1);
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
        auto toRet = std::pair{in, (node == 0? a : (node == 1? b : (node == 2? c : d)))};
        return bsp_all_send(toRet);
    };

    std::vector<bsp_function<std::pair<std::vector<int>, std::vector<int>>>> phase3funcs(4);
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

    printvvect(bspfun.run());
}

