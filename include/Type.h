#ifndef BLAZE_TYPE_H
#define BLAZE_TYPE_H

#include <cstdint>
#include <vector>
#include <map>
#include "galois/Bag.h"
#include "Worklist.h"
#include "galois/substrate/SimpleLock.h"

typedef uint32_t VID;
#define VID_BITS 2
#define EDGE_WIDTH_BITS 2

typedef int EDGEDATA;
//typedef float EDGEDATA;
struct EdgePair {
    VID dst;
    EDGEDATA data;
};

struct graph_header {
    uint64_t unused;
    uint64_t size_of_edge;
    uint64_t num_nodes;
    uint64_t num_edges;
};

typedef uint32_t PAGEID;
using VidRange = std::pair<VID, VID>;

struct IoItem {
    int     disk_id;
    PAGEID  page;
    int     num;
    char*   buf;
    bool    hit;
    struct hitchhiker*   _hit_buf;
    uint64_t*   pages_id;
    IoItem(int d, PAGEID p, int n, char* b, bool h, struct hitchhiker* c, uint64_t* i): disk_id(d), page(p), num(n), 
                                                                 buf(b), hit(h), _hit_buf(c), pages_id(i) {}
    IoItem(int d, PAGEID p, int n, char* b, bool h): disk_id(d), page(p), num(n), buf(b),hit(h) {}
};

using PageReadList = std::vector<std::pair<PAGEID, char *>>;

using Mutex = galois::substrate::SimpleLock;

typedef uint32_t FLAGS;
const FLAGS no_output           = 0x01;
const FLAGS prop_blocking       = 0x10;
//zhengxd: hitchhike flag
const FLAGS hit                = 0x01;

inline bool should_output(const FLAGS& flags) {
    return !(flags & no_output);
}

inline bool use_prop_blocking(const FLAGS& flags) {
    return flags & prop_blocking;
}

inline bool is_hitchhike(const FLAGS& flags) {
    return flags & hit;
}
enum ComputeWorkerRole { NORMAL, BIN, ACCUMULATE };

#endif // BLAZE_TYPES_H
