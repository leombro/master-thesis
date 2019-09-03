//
// Created by Orlando Leombruni on 30/08/2019.
//
#include "../lib/ff_bsp.hpp"
#include <chrono>
#include <sstream>

std::vector<double> leastsquares(int h0, int h1, double t[]) {
    std::vector<double> ret(2);
    double nh = static_cast<double>(h1 - h0 + 1);

    double sumt = 0.0;
    double sumth = 0.0;
    for (int h = h0; h <= h1; h++) {
        sumt += t[h];
        sumth += t[h]*h;
    }

    double sumh = static_cast<double>(h1*h1 - h0*h0 + h1 + h0)/2.0;
    double sumhh = static_cast<double>(h1 * (h1+1) * (2*h1+1) - (h0 - 1) * h0 * (2*h0 - 1))/6.0;

    if (abs(nh) > abs(sumh)) {
        double a = sumh/nh;
        ret[0] = (sumth - a*sumt) / (sumhh - a*sumh);
        ret[1] = (sumt - sumh*ret[0])/nh;
    } else {
        double a = nh/sumh;
        ret[0] = (sumt - a*sumth) / (sumh - a*sumhh);
        ret[1] = (sumth - sumhh*ret[0])/sumh;
    }

    return ret;
}

int main(int argc, char* argv[]) {

    double r = 0.0;
    double MEGA = 1000000.0;

    int maxN = 0, maxH = 0, n_iters = 0, n_procs = 0;

    if (argc < 2) {
        std::cout << "Usage: BSPbench <P> (NITERS) (MAXN) (MAXH)" << std::endl;
        std::cout << "<..> are obligatory parameters" << std::endl;
        std::cout << "(..) are optional" << std::endl;
        return 1;
    }

    std::istringstream nprocs_iss{argv[1]};

    if (!(nprocs_iss >> n_procs) || n_procs < 1) {
        std::cout << "invalid number of processors" << std::endl;
        return 1;
    }

    if (argc > 2) {
        if (argc > 3) {
            if (argc > 4) {
                std::istringstream maxh_iss{argv[4]};
                if (!(maxh_iss >> maxH)) {
                    std::cout << "error in reading maxH" << std::endl;
                    return 1;
                }
            } else {
                maxH = 128;
            }
            std::istringstream maxn_iss{argv[3]};
            if (!(maxn_iss >> maxN) || maxN < 1) {
                std::cout << "invalid maxN" << std::endl;
                return 1;
            }
        } else {
            maxN = 1024;
            maxH = 128;
        }
        std::istringstream niters_iss{argv[2]};
        if (!(niters_iss >> n_iters) || n_iters < 1) {
            std::cout << "invalid maxN" << std::endl;
            return 1;
        }
    } else {
        n_iters = 100;
        maxN = 1024;
        maxH = 128;
    }

    int n = 1, h = 2;

    std::vector<bsp_function<int>> daxpy(n_procs);
    std::fill(daxpy.begin(), daxpy.end(), [&](int, node_id) {
        double alpha = 1.0/3.0;
        double beta = 4.0/9.0;
        std::vector<double> x(maxN);
        std::vector<double> y(maxN);
        std::vector<double> z(maxN);
        for (int i = 0; i < n; i++) {
            z[i] = y[i] = x[i] = (double) i;
        }

        auto a = std::chrono::high_resolution_clock::now();
        for (int iter = 0; iter < n_iters; iter++) {
            for (int i = 0; i < n; i++)
                y[i] += alpha * x[i];
            for (int i = 0; i < n; i++)
                z[i] -= beta * x[i];
        }
        auto b = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(b - a).count();
        double seconds = elapsed/1000.0;

        return bsp_any_send(seconds);
    });
    bsp_superstep<int, double> step1_daxpy(daxpy);

    std::vector<bsp_function<std::vector<double>>> time_comp(1);
    time_comp[0] = [&, rparam = &r](const std::vector<double>& times, node_id) -> bsp_send {
        double mintime = times[0];
        double maxtime = times[0];
        for (const auto& t: times) {
            mintime = std::min(mintime, t);
            maxtime = std::max(maxtime, t);
        }
        if (mintime > 0.0) {
            int nflops = 4 * n_iters * n;
            double r1 = 0.0;
            for (const auto& t: times) {
                r1 += static_cast<double>(nflops) / t;
            }
            r1 /= static_cast<double>(n_procs);
            *rparam = r1;
            std::cout <<    "n= " << n
                      << " min= " << nflops/(mintime*MEGA)
                      << " max= " << nflops/(maxtime*MEGA)
                      <<  " av= " << r1/MEGA << " Mflop/s " << std::endl;
            std::cout << " fool= " << times.at(times.size()-1) + times[0] << std::endl;
        } else {
            std::cout << "minimum time is 0" << std::endl;
        }
        int dummy = 0;
        return bsp_all_send(dummy);
    };
    bsp_superstep<std::vector<double>, int> step2_timecalc(time_comp, [&, ni = &n](int, sstep_id id){
        sstep_id next;
        if (*ni < maxN) next = id - 1;
        else {
            if (maxH == 0) next = id + 6;
            else next = id + 1;
        }
        *ni *= 2;
        return next;
    });

    std::vector<int> dests[n_procs];
    int h_iter = 0;
    std::chrono::time_point<std::chrono::high_resolution_clock> begin_time, end_time;
    double t[maxH+1];

    std::vector<bsp_function<int>> h1prep(n_procs);
    std::fill(h1prep.begin(), h1prep.end(), [&](int, node_id id) {
        dests[id].clear();
        if (n_procs == 1) dests[id].emplace_back(0);
        else {
            int proc = (id + 1) % n_procs;
            dests[id].emplace_back(proc);
        }
        int dum = 0;
        return bsp_send(dum, id);
    });
    bsp_superstep<int> step3_h1prep(h1prep, [&, start = &begin_time](int, sstep_id id){
        *start = std::chrono::high_resolution_clock::now();
        return id+1;
    });

    std::vector<bsp_function<int>> h1loop(n_procs);
    std::fill(h1loop.begin(), h1loop.end(), [&](int, node_id id) {
        int i = 0;
        return bsp_send(i, dests[id][0]);
    });
    bsp_superstep<int> step4_h1loop(h1loop, [&, nh = &h_iter, end = &end_time](int, sstep_id id){
        *nh += 1;
        std::cout << *nh << std::endl;
        if (*nh <= n_iters) return id;
        else {
            *end = std::chrono::high_resolution_clock::now();
            *nh = 0;
            return id + 1;
        }
    });

    std::vector<bsp_function<int>> dummyvect(n_procs); // for type correctness
    std::fill(dummyvect.begin(), dummyvect.end(), [&](int, node_id id){
        if (id == 0) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time).count();
            double time = static_cast<double>(elapsed)/1000.0;
            t[0] = (time * r) / n_iters;
            std::cout <<  "Time of 1-relation= " << time/n_iters << " sec= " << t[0] << " flops" << std::endl;
        }
        std::vector<int> d;
        return bsp_send(d, id);
    });
    bsp_superstep<int, std::vector<int>> step5_dummy(dummyvect);

    std::vector<bsp_function<std::vector<int>>> hxprep(n_procs);
    std::fill(hxprep.begin(), hxprep.end(), [&](const std::vector<int>&, node_id id) {
        dests[id].clear();
        for (int i = 0; i < h; i++) {
            if (n_procs == 1) {
                dests[id].emplace_back(0);
            } else {
                int proc = ( id+1 + i%(n_procs-1)) % n_procs;
                dests[id].emplace_back(proc);
            }
        }
        std::vector<int> dummy;
        return bsp_send(dummy, id);
    });
    bsp_superstep<std::vector<int>> step6_hxprep(hxprep, [start = &begin_time](const std::vector<int>&, sstep_id id){
        *start = std::chrono::high_resolution_clock::now();
        return id+1;
    });


    std::vector<bsp_function<std::vector<int>>> hxloop(n_procs);
    std::fill(hxloop.begin(), hxloop.end(), [&](const std::vector<int>&, node_id id){
        int i = 0;
        bsp_multisend ms(i, dests[id][0]);
        for (i = 1; i < h; i++) ms.add(i, dests[id][i]);
        return ms;
    });
    bsp_superstep<std::vector<int>, int> step7_hxloop(hxloop, [&, end = &end_time, iterations = &h_iter, hparam = &h](int, sstep_id id){
        *iterations += 1;
        if (*hparam == maxH) std::cout << "h is " << *hparam << " iter is " << *iterations << std::endl;
        if (*iterations > n_iters) {
            *end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - begin_time).count();
            double time = static_cast<double>(elapsed)/1000.0;
            t[*hparam] = (time * r) / n_iters;
            std::cout <<  "Time of " << *hparam << "-relation= " << time/n_iters << " sec= " << t[*hparam] << " flops" << std::endl;
            *hparam += 1;
            if (*hparam > maxH) return id+1;
            else {
                *iterations = 0;
                return id-1;
            }
        } else return id;
    });

    std::vector<bsp_function<std::vector<int>>> final(n_procs);
    std::fill(final.begin(), final.end(), [&](const std::vector<int>&, node_id id){
        if (id == 0) {
            std::vector<double> temp;

            temp = leastsquares(0, n_procs, t);
            std::cout << "Range h=0 to p   : g=" << temp[0] << ", l= " << temp[1] << std::endl;
            temp = leastsquares(n_procs, maxH, t);
            std::cout << "Range h=p to HMAX: g=" << temp[0] << ", l= " << temp[1] << std::endl;

            std::cout << "The bottom line for this BSP computer is:" << std::endl;
            std::cout << "p= " << n_procs << ", r= " << r/MEGA << " Mflop/s, g= " << temp[0] << ", l= " << temp[1] << std::endl;
        }
        int dum = 0;
        return bsp_send(dum, id);
    });
    bsp_superstep<std::vector<int>, int> step8_final(final, [](auto, sstep_id id) {return STOP;});

    std::vector<int> dummyInput(n_procs);
    std::fill(dummyInput.begin(), dummyInput.end(), 1);

    bsp<int> bspbench(dummyInput);
    bspbench.add_superstep(step1_daxpy);
    bspbench.add_superstep(step2_timecalc);
    bspbench.add_superstep(step3_h1prep);
    bspbench.add_superstep(step4_h1loop);
    bspbench.add_superstep(step5_dummy);
    bspbench.add_superstep(step6_hxprep);
    bspbench.add_superstep(step7_hxloop);
    bspbench.add_superstep(step8_final);

    bspbench.run();
    return 0;
}