//
// Created by Orlando Leombruni on 2019-07-14.
//

#include "ff_bsp.hpp"
#include <iostream>
#include <vector>
#include <functional>

int main() {

    std::vector<int> input{1};

    std::vector<std::function<bsp_send(int, node_id)>> phase1 = {[](int a, node_id id){
        return bsp_send(a, 0);
    }};

    std::vector<std::function<bsp_send(int, node_id)>> phase2 = {[](int a, node_id id){
        int b = a+1;
        return bsp_send(b, 0);
    }};

    bsp_superstep<int, int> first_sstep(phase1);
    bsp_superstep<int, int> second_sstep(phase2, [](int el, sstep_id id){
        return (el == 10? STOP : id);
    });

    bsp<int, int> bulk(input);
    bulk.add_superstep(first_sstep);
    bulk.add_superstep(second_sstep);

    std::vector<int> result{bulk.run()};

    std::cout << "Result is " << result[0] << std::endl;

    return 0;
}