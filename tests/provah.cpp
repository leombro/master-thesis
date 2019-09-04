//
// Created by Orlando Leombruni on 04/09/2019.
//

#include "../lib/ff_bsp.hpp"

int main() {

    double MEGA = 1000000.0;

    int NITERS = 100;
    int NTHREADS = 4;
    int iter = 0;

    std::vector<bsp_function<int>> hloop(NTHREADS);
    std::fill(hloop.begin(), hloop.end(), [&](int, node_id id) {
        int i = 0;
        return bsp_send(i, (id+iter+1)%NTHREADS);
    });
    bsp_superstep<int> step(hloop, [&, iter = &iter](int, sstep_id id) {
        if (*iter >= NITERS) return STOP;
        *iter += 1;
        return id;
    });

    std::vector<int> input(NTHREADS);
    std::fill(input.begin(), input.end(), 5);

    bsp<int> mBsp(input);
    mBsp.add_superstep(step);

    auto begin = std::chrono::high_resolution_clock::now();
    mBsp.run();
    auto end = std::chrono::high_resolution_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end - begin);
    std::cout << "spent total: " << dur.count()/MEGA << " - per iter: " << dur.count()/(NITERS*MEGA) << std::endl;

    std::cout << "hloop size is " << hloop.size() << std::endl;

    std::vector<bsp_function<int>> base(NTHREADS);
    std::fill(base.begin(), base.end(), [](int, node_id id) {
        int i = 0;
        return bsp_send(i, id);
    });
    std::vector<std::vector<bsp_function<int>>> ssteps(NITERS - 1);
    std::fill(ssteps.begin(), ssteps.end(), base);

    bsp<int> mBsp2(input);
    for (auto& mstep: ssteps) {
        bsp_superstep<int> sstep(mstep);
        mBsp2.add_superstep(sstep);
    }
    bsp_superstep<int> last(base, [](int, sstep_id){return STOP;});
    mBsp2.add_superstep(last);

    auto begin2 = std::chrono::high_resolution_clock::now();
    mBsp2.run();
    auto end2 = std::chrono::high_resolution_clock::now();
    auto dur2 = std::chrono::duration_cast<std::chrono::microseconds>(end2 - begin2);
    std::cout << "spent total: " << dur2.count()/MEGA << " - per iter: " << dur2.count()/(NITERS*MEGA) << std::endl;
}