//
// Created by Orlando Leombruni on 30/08/2019.
//

#include "../lib/ff_bsp.hpp"
#include <sstream>

int main() {

    std::vector<int> dummyInput{1,2,3,4,5,6,7,8,9,0};

    std::vector<bsp_function<int>> v1(10);
    std::fill(v1.begin(), v1.end(), [](int, node_id id) {
        int a = 5;
        return bsp_send(a, 6, false);
    });
    bsp_superstep<int, int> step1(v1);

    std::vector<bsp_function<std::vector<int>>> v2(10);
    std::fill(v2.begin(), v2.end(), [](const std::vector<int>& v, node_id id) {
        std::ostringstream oss;
        oss << "Hi I'm " << id << " and this is my vector: [ ";
        for (const auto& i: v) oss << i << " ";
        oss << std::endl;
        std::string r = oss.str();
        return bsp_send(r, id, false);
    });
    bsp_superstep<std::vector<int>, std::string> step2(v2, [](auto, auto){return STOP;});


    bsp<int, std::string> mBsp(dummyInput);
    mBsp.add_superstep(step1);
    mBsp.add_superstep(step2);
    std::vector<std::string> out{mBsp.run()};

    for (const auto& o: out) std::cout << o;
}