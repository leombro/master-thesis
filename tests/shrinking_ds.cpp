//
// Created by Orlando Leombruni on 2019-07-14.
//

#include "ff_bsp.hpp"
#include <iostream>
#include <vector>
#include <functional>

int main() {

    std::vector<std::vector<int>> input(4);
    std::vector<int> data(65536);

    std::fill(data.begin(), data.end(), 1);
    std::fill(input.begin(), input.end(), data);

    std::vector<std::function<bsp_send(std::vector<int>, node_id)>> step(4);

    std::fill(step.begin(), step.end(), [](const std::vector<int>& in, node_id id){
        size_t sz = in.size();
        auto first = in.begin();
        auto last = in.begin() + sz/2;
        std::vector<int> ret(first, last);
        return bsp_send(ret, id);
    });

    bsp_superstep<std::vector<int>> sstep(step, [](const std::vector<int>& in, sstep_id id){
        return (in.size() < 200? STOP : id);
    });

    std::cout << "starting" << std::endl;
    bsp<std::vector<int>> bulk(input);
    bulk.add_superstep(sstep);

    std::vector<std::vector<int>> result{bulk.run()};

    std::cout << "Vector size is " << result[0].size() << std::endl;
}