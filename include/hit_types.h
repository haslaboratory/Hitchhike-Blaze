#ifndef HIT_TYPES_H
#define HIT_TYPES_H

#include "../apps/include/boilerplate.h"


typedef unsigned long long ptr__m;
typedef unsigned long long pageid_m;
typedef unsigned long long length_m; //page
typedef unsigned long long offset_m; //page


#define PAGE_SIZE 4096
#define IO_INFO  128       // (128 *4 = 512KB)
#define MAX_BIO_SIZE (hitSize * 4096)       // Max size 128KB
#define PTR_SIZE sizeof(ptr__m)


/* MASK to prevent out of bounds memory access*/
#define BLOCK_OFFSET_MASK SCRATCH_SIZE
#define INFTY_MAX ((ptr__m)1 << 63) //INFTY_64

// page
#define PAGE_MAX 128
#define INDEX_MASK (IO_INFO - 1)

#endif /* HIT_TYPES_H */
