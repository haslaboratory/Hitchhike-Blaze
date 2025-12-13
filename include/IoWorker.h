#ifndef BLAZE_IO_WORKER_H
#define BLAZE_IO_WORKER_H

#include "Type.h"
#include "Graph.h"
#include "boilerplate.h"
#include "hit_types.h"
#include "galois/Bag.h"
#include "Param.h"
#include "IoSync.h"
#include "Synchronization.h"
#include "AsyncIo.h"
#include "Queue.h"
#include "Param.h"
#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include "helpers.h"

namespace blaze {

class Runtime;

class IoWorker {
 public:
    IoWorker(int id,
             uint64_t buffer_size,
             MPMCQueue<IoItem*>* out)
        :   _id(id),
            _buffered_tasks(out),
            _queued(0), _sent(0), _received(0), _requested_all(false),
            _total_bytes_accessed(0),_total_bytes_accessed_hit(0), _time(0.0),
            _queued_hit(0),_sent_hit(0)
    {
        initAsyncIo();
        initHitchhike();
        _num_buffer_pages = (int64_t)buffer_size / PAGE_SIZE;
    }

    ~IoWorker() {
        deinitAsyncIo();
        deinitHitchhike();
    }

    void initAsyncIo() {
        _ctx = 0;
        int ret = io_setup(IO_QUEUE_DEPTH, &_ctx);
        assert(ret == 0);
        _iocb = (struct iocb*)calloc(IO_QUEUE_DEPTH, sizeof(*_iocb));
        _iocbs = (struct iocb**)calloc(IO_QUEUE_DEPTH, sizeof(*_iocbs));
        _events = (struct io_event*)calloc(IO_QUEUE_DEPTH, sizeof(*_events));
    }

    void deinitAsyncIo() {
        io_destroy(_ctx);
        free(_iocb);
        free(_iocbs);
        free(_events);
    }
        
    void initHitchhike ()
    {
        _hit_bufs_tmp = (struct hitchhiker**)calloc(IO_QUEUE_DEPTH, sizeof(ptr__m));
    }
    
    void deinitHitchhike() {
        free(_hit_bufs_tmp);
    }


    void run(int fd, bool dense_all, Bitmap* page_bitmap, CountableBag<PAGEID>* sparse_page_frontier,
            Synchronization& sync, IoSync& io_sync, FLAGS& hitchhike) {

        _hitchhike = hitchhike;
        _fd = fd;

        sync.set_num_free_pages(_id, _num_buffer_pages);

        if (dense_all) {
            run_dense_all(page_bitmap, sync, io_sync);
        }

        if (sparse_page_frontier) {
            run_sparse(sparse_page_frontier, page_bitmap, sync, io_sync);

        } else {
            run_dense(page_bitmap, sync, io_sync);
        }
    }

    uint64_t getBytesAccessed() const {
        return _total_bytes_accessed;
    }

    uint64_t getBytesAccessed_hit() const {
        return _total_bytes_accessed_hit;
    }

    void initState() {
        _queued = 0;
        _sent = 0;
        _received = 0;
        _requested_all = false;
        _total_bytes_accessed = 0;
        _total_bytes_accessed_hit = 0;
        _time = 0;
    }

 private:


    void run_dense_all(Bitmap* page_bitmap, Synchronization& sync, IoSync& io_sync) {
        IoItem* done_tasks[IO_QUEUE_DEPTH];
        int received;

        PAGEID beg = 0;
        const PAGEID end = page_bitmap->get_size();

        while (!_requested_all || _received < _queued) {
            submitTasks_dense_all(beg, end, sync, io_sync);
            received = receiveTasks(done_tasks);
            dispatchTasks(done_tasks, received);
        }
    }

    void run_dense(Bitmap* page_bitmap, Synchronization& sync, IoSync& io_sync) {
        IoItem* done_tasks[IO_QUEUE_DEPTH];
        int received;

        PAGEID beg = 0;
        const PAGEID end = page_bitmap->get_size();

        while (!_requested_all || _received < _queued) {
            if((_queued - _received) < IO_QUEUE_DEPTH){
                if(is_hitchhike(_hitchhike)){
                    submitTasks_dense_hit(page_bitmap, beg, end, sync, io_sync);
                } else {
                    submitTasks_dense(page_bitmap, beg, end, sync, io_sync);
                }
            }

            received = receiveTasks(done_tasks);
            dispatchTasks(done_tasks, received);
        }
    }

    void run_sparse(CountableBag<PAGEID>* sparse_page_frontier, Bitmap* page_bitmap, 
                            Synchronization& sync, IoSync& io_sync) {
        IoItem* done_tasks[IO_QUEUE_DEPTH];
        int received;

        auto beg = sparse_page_frontier->begin();
        auto const end = sparse_page_frontier->end();

        while (!_requested_all || _received < _queued) {
            
            if((_queued - _received) < IO_QUEUE_DEPTH){
                if(is_hitchhike(_hitchhike)){
                    submitTasks_sparse_hit(beg, end, page_bitmap, sync, io_sync);
                } else {
                    submitTasks_sparse(beg, end, page_bitmap, sync, io_sync);
                }
            }

            received = receiveTasks(done_tasks);
            dispatchTasks(done_tasks, received);
        }
    }

    void submitTasks_dense_all(PAGEID& beg, const PAGEID& end,
                            Synchronization& sync, IoSync& io_sync)
    {
        char* buf;
        off_t offset;
        void* data;

        while (beg < end && (_queued - _sent) < IO_QUEUE_DEPTH) {
            // check continuous pages up to 16kB
            PAGEID page_id = beg;
            uint64_t num_pages = IO_MAX_PAGES_PER_REQ;
            if (beg + num_pages > end)
                num_pages = end - beg;

            // wait until free pages are available
            while (sync.get_num_free_pages(_id) < num_pages) {}
            sync.add_num_free_pages(_id, (int64_t)num_pages * (-1));

            uint64_t len = num_pages * PAGE_SIZE;
            buf = (char*)aligned_alloc(PAGE_SIZE, len);
            offset = (uint64_t)page_id * PAGE_SIZE;
            IoItem* item = new IoItem(_id, page_id, num_pages, buf,0);
            enqueueRequest(buf, len, offset, item);

            beg += num_pages;
        }

        if (beg >= end) _requested_all = true;

        if (_queued - _sent == 0) return;

        for (size_t i = 0; i < _queued - _sent; i++) {
            _iocbs[i] = &_iocb[(_sent + i) % IO_QUEUE_DEPTH];
        }

        int ret = io_submit(_ctx, _queued - _sent, _iocbs);
        if (ret > 0) {
            _sent += ret;
        }
    }

    void submitTasks_dense(Bitmap* page_bitmap, PAGEID& beg, const PAGEID& end,
                        Synchronization& sync, IoSync& io_sync)
    {
        char* buf;
        off_t offset;
        void* data;

        while (beg < end && (_queued - _sent) < IO_QUEUE_DEPTH) {
            // skip an entire word in bitmap if possible
            // note: this is quite effective to keep IO queue busy
            if (!page_bitmap->get_word(Bitmap::word_offset(beg))) {
                beg = Bitmap::pos_in_next_word(beg);
                continue;
            }

            if (!page_bitmap->get_bit(beg)) {
                beg++;
                continue;

            } else {
                // check continuous pages up to 16kB
                PAGEID page_id = beg;
                uint64_t num_pages = 1;
                while (page_bitmap->get_bit(++beg)
                             && beg < end 
                             && num_pages < IO_MAX_PAGES_PER_REQ)
                {
                    num_pages++;
                }

                // wait until free pages are available
                while (sync.get_num_free_pages(_id) < num_pages) {}
                sync.add_num_free_pages(_id, (int64_t)num_pages * (-1));
                
                uint64_t len = num_pages * PAGE_SIZE;
                buf = (char*)aligned_alloc(PAGE_SIZE, len);
                offset = (uint64_t)page_id * PAGE_SIZE;
                IoItem* item = new IoItem(_id, page_id, num_pages, buf,0);
                enqueueRequest(buf, len, offset, item);
                // zhengxd: 打印page 连续性
                // printf(" %ld \n",num_pages);
            }
        }

        if (beg >= end) _requested_all = true;

        if (_queued - _sent == 0) return;

        for (size_t i = 0; i < _queued - _sent; i++) {
            _iocbs[i] = &_iocb[(_sent + i) % IO_QUEUE_DEPTH];
        }

        int ret = io_submit(_ctx, _queued - _sent, _iocbs);
        if (ret > 0) {
            _sent += ret;
        }
    }

    void submitTasks_sparse(CountableBag<PAGEID>::iterator& beg,
                            const CountableBag<PAGEID>::iterator& end,
                            Bitmap* page_bitmap,
                            Synchronization& sync, IoSync& io_sync)
    {
        char* buf;
        off_t offset;
        void* data;
        PAGEID page_id;

        while (beg != end && (_queued - _sent) < IO_QUEUE_DEPTH) {
            page_id = *beg;

            if (page_bitmap->get_bit(page_id)) {
                beg++;
                continue;
            }

            // wait until free pages are available
            while (sync.get_num_free_pages(_id) < 1) {}
            sync.add_num_free_pages(_id, (int64_t)(-1));
   

            buf = (char*)aligned_alloc(PAGE_SIZE, PAGE_SIZE);
            offset = (uint64_t)page_id * PAGE_SIZE;
            IoItem* item = new IoItem(_id, page_id, 1, buf,0);
            enqueueRequest(buf, PAGE_SIZE, offset, item);
            page_bitmap->set_bit(page_id);

            beg++;
        }

        if (beg == end) _requested_all = true;

        if (_queued - _sent == 0) return;

        for (size_t i = 0; i < _queued - _sent; i++) {
            _iocbs[i] = &_iocb[(_sent + i) % IO_QUEUE_DEPTH];
        }

        int ret = io_submit(_ctx, _queued - _sent, _iocbs);
        if (ret > 0) {
            _sent += ret;
        }
    }

    void hit_dense(Bitmap* page_bitmap, PAGEID& beg, const PAGEID& end, uint64_t used_pages,
                    struct hitchhiker* _hit_buf, uint64_t *pages_id){

        PAGEID page_id;
        uint64_t cur_pages;
        uint64_t offset = 0;
        uint64_t offset_pages = 0, index = 0;
        _buffer_len = used_pages * PAGE_SIZE;

      
        while (beg < end && (index <= HIT_NUMBER) && _buffer_len < MAX_BIO_SIZE) {
            // skip an entire word in bitmap if possible
            // note: this is quite effective to keep IO queue busy
            if (!page_bitmap->get_word(Bitmap::word_offset(beg))) {
                beg = Bitmap::pos_in_next_word(beg);
                continue;
            }

            if (!page_bitmap->get_bit(beg)) {
                beg++;
                continue;
            } else {
                // check continuous pages up to MAX_BIO_SIZE
                // check beg is not host io
                page_id = beg;
                cur_pages = 1;
                offset_pages += cur_pages;
                offset = (uint64_t)page_id * PAGE_SIZE;
                _buffer_len += cur_pages * PAGE_SIZE;

                pages_id[index] = page_id;
                _hit_buf->addr[index] = offset;
                //zhengxd: size always == 4096
                _hit_buf->size = PAGE_SIZE;
                _hit_buf->max = index;
                _hit_buf->in_use = 1;
                _hit_buf->iov_use = 0;
                index++;
                beg++;
                _queued_hit++;
            }
        }
    }

    void submitTasks_dense_hit(Bitmap* page_bitmap, PAGEID& beg, const PAGEID& end,
                        Synchronization& sync, IoSync& io_sync)
    {
        char* buf;
        uint64_t offset;
        void* data;
        PAGEID page_id;
        uint64_t index = 0;
        uint64_t num_pages;
        uint64_t sent = 0;

        while (beg < end && (_queued - _sent) < IO_QUEUE_DEPTH) {
            // skip an entire word in bitmap if possible
            // note: this is quite effective to keep IO queue busy
            if (!page_bitmap->get_word(Bitmap::word_offset(beg))) {
                beg = Bitmap::pos_in_next_word(beg);
                continue;
            }

            if (!page_bitmap->get_bit(beg)) {
                beg++;
                continue;

            } else {
                // check continuous pages up to 16kB
                page_id = beg;
                num_pages = 1;
                beg++;
                _queued_hit++;
                // struct hitchhike
                uint64_t *pages_id = (uint64_t *)calloc(1,128 *sizeof(uint64_t));
                struct hitchhiker* _hit_buf = (struct hitchhiker*)calloc(1, sizeof(struct hitchhiker));
     
                hit_dense(page_bitmap,beg,end,num_pages,_hit_buf,pages_id);
                uint64_t hit_pages = _buffer_len / PAGE_SIZE;

                // wait until free pages are available
                while (sync.get_num_free_pages(_id) < hit_pages) {}
                sync.add_num_free_pages(_id, (int64_t)hit_pages * (-1));
                buf = (char*)aligned_alloc(PAGE_SIZE, _buffer_len);
                offset = (uint64_t)page_id * PAGE_SIZE;

                // aio init
                IoItem* item = new IoItem(_id, page_id, 1, buf,1, _hit_buf,pages_id);
                enqueueRequest_hit(buf, PAGE_SIZE, _buffer_len, offset, item);
                _hit_bufs_tmp[index] = _hit_buf;
                index++;
                // dump_page((unsigned char *)(&_hit_buf[index]), 4096);
                sent += (_buffer_len/PAGE_SIZE);
            }
        }

        if (beg >= end) _requested_all = true;

        if (_queued - _sent == 0) return;

        for (size_t i = 0; i < _queued - _sent; i++) {
            _iocbs[i] = &_iocb[(_sent + i) % IO_QUEUE_DEPTH];
        }

        int ret = io_submit_hit(_ctx, _queued - _sent, _iocbs, _hit_bufs_tmp);
        if (ret > 0) {
            _sent += ret;
            _sent_hit += sent;
        }
        memset(_hit_bufs_tmp, 0, (IO_QUEUE_DEPTH * sizeof(ptr__m)));
    }

    void hit_sparse(Bitmap* page_bitmap, CountableBag<PAGEID>::iterator& beg,const CountableBag<PAGEID>::iterator& end, 
                        uint64_t used_pages, struct hitchhiker* _hit_buf, uint64_t* pages_id){

        uint64_t offset = 0, index = 0;
        _buffer_len = used_pages * PAGE_SIZE;
        PAGEID page_id;

        while ((beg != end) && (index <= HIT_NUMBER) && (_buffer_len < MAX_BIO_SIZE)) {
  
            page_id = *beg;
            if (page_bitmap->get_bit(page_id)) {
                beg++;
                continue;
            } else {
                // check continuous pages up to 128KB
                // check beg is not host io
                offset = (uint64_t)page_id * PAGE_SIZE;
                _buffer_len += PAGE_SIZE;

                pages_id[index] = (uint64_t)page_id;
                _hit_buf->addr[index] = offset;
                // size always == 4096
                _hit_buf->size = PAGE_SIZE;
                _hit_buf->max = index;
                _hit_buf->in_use = 1;
                _hit_buf->iov_use = 0;
                index++;
                page_bitmap->set_bit(page_id);
                _queued_hit++;
            }
        }
    }

    void submitTasks_sparse_hit(CountableBag<PAGEID>::iterator& beg,
                            const CountableBag<PAGEID>::iterator& end,
                            Bitmap* page_bitmap,
                            Synchronization& sync, IoSync& io_sync)
    {
        char* buf;
        uint64_t offset;
        void* data;
        PAGEID page_id;
        uint64_t index = 0;
        uint64_t sent = 0;

        while (beg != end && (_queued - _sent) < IO_QUEUE_DEPTH) {
            page_id = *beg;

            if (page_bitmap->get_bit(page_id)) {
                beg++;
                continue;
            }
            page_bitmap->set_bit(page_id);
            beg++;
            _queued_hit++;

            struct hitchhiker* _hit_buf = (struct hitchhiker*)calloc(1,sizeof(struct hitchhiker));
            uint64_t *pages_id = (uint64_t *)calloc(1,128 *sizeof(uint64_t));

            hit_sparse(page_bitmap,beg,end,1,_hit_buf,pages_id);
            uint64_t hit_pages = _buffer_len / PAGE_SIZE;

            while (sync.get_num_free_pages(_id) < hit_pages) {}
            sync.add_num_free_pages(_id, hit_pages*(-1));
            buf = (char*)aligned_alloc(PAGE_SIZE, _buffer_len);
            offset = (uint64_t)page_id * PAGE_SIZE;

            // aio init
            IoItem* item = new IoItem(_id, page_id, 1, buf,1, _hit_buf,pages_id);
            enqueueRequest_hit(buf, PAGE_SIZE, _buffer_len, offset, item);;
            _hit_bufs_tmp[index] = _hit_buf;
            index++;
            sent += (_buffer_len/PAGE_SIZE);
            // debug info
            // dump_page((unsigned char *)(_hit_buf), sizeof(struct hitchhiker));
           
        }

        if (beg == end) _requested_all = true;

        if (_queued - _sent == 0) return;

        for (size_t i = 0; i < _queued - _sent; i++) {
            _iocbs[i] = &_iocb[(_sent + i) % IO_QUEUE_DEPTH];
        }

        int ret = io_submit_hit(_ctx, _queued - _sent, _iocbs, _hit_bufs_tmp);
        if (ret > 0) {
            _sent += ret;
            _sent_hit += sent;
            
        }
        memset(_hit_bufs_tmp, 0, (IO_QUEUE_DEPTH * sizeof(ptr__m)));
    }

    void enqueueRequest(char* buf, size_t len, off_t offset, void* data) {
        uint64_t idx = _queued % IO_QUEUE_DEPTH;
        struct iocb* pIocb = &_iocb[idx];
        memset(pIocb, 0, sizeof(*pIocb));
        pIocb->aio_fildes = _fd;
        pIocb->aio_lio_opcode = IOCB_CMD_PREAD;
        pIocb->aio_buf = (uint64_t)buf;
        pIocb->aio_nbytes = len;
        pIocb->aio_offset = offset;
        pIocb->aio_data = (uint64_t)data;
        _queued++;

        _total_bytes_accessed += len;
    }


    void enqueueRequest_hit(char* buf, size_t data_len, size_t _buffer_len, off_t offset, void* data) {
        uint64_t idx = _queued % IO_QUEUE_DEPTH;
        struct iocb* pIocb = &_iocb[idx];
        memset(pIocb, 0, sizeof(*pIocb));
        pIocb->aio_fildes = _fd;
        pIocb->aio_lio_opcode = IOCB_CMD_PREAD;
        pIocb->aio_buf = (uint64_t)buf;
        pIocb->aio_nbytes = _buffer_len;
        pIocb->aio_offset = offset;
        pIocb->aio_data = (uint64_t)data;
        pIocb->aio_dsize = data_len;
        _queued++;

        _total_bytes_accessed += data_len;
        _total_bytes_accessed_hit += _buffer_len;
        _buffer_len = 0;
    }    

    int receiveTasks(IoItem** done_tasks) {
        if (_requested_all && _sent == _received) return 0;

        unsigned min = 1;
        unsigned max = IO_QUEUE_DEPTH;

        int received = io_getevents(_ctx, min, max, _events, NULL);
        assert(received <= max);
        assert(received >= 0);

        for (int i = 0; i < received; i++) {
            assert(_events[i].res > 0);
            auto item = reinterpret_cast<IoItem*>(_events[i].data);
            done_tasks[i] = item;
        }
        _received += received;

        return received;
    }

    void dispatchTasks(IoItem** done_tasks, int received) {
        if (received > 0)
            _buffered_tasks->enqueue_bulk(done_tasks, received);
    }

 protected:
    int                     _id;
    MPMCQueue<IoItem*>*     _buffered_tasks;

    // To control IO
    int                     _fd;
    uint64_t                _queued;
    uint64_t                _sent;
    uint64_t                _queued_hit;
    uint64_t                _sent_hit;
    uint64_t                _received;
    bool                    _requested_all;
    int64_t                 _num_buffer_pages;
    // For statistics
    uint64_t                _total_bytes_accessed;
    uint64_t                _total_bytes_accessed_hit;
    double                  _time;

    aio_context_t                       _ctx;
    struct iocb*                        _iocb;
    struct iocb**                       _iocbs;
    struct io_event*                    _events;

    // hit 
    struct hitchhiker*                   _hit_buf_tmp;
    struct hitchhiker**                  _hit_bufs_tmp;
    uint64_t                _scratch_pages;
    uint64_t                _hitchhike;
    size_t                  _buffer_len; // bytes


    std::chrono::time_point<std::chrono::steady_clock>  _time_start;
    std::chrono::time_point<std::chrono::steady_clock>  _time_end;
    std::chrono::duration<double> duration_hit;
    std::chrono::duration<double> duration_io_submit;
    std::chrono::duration<double> duration_received;
    std::chrono::duration<double> duration_dispatch;

};

} // namespace blaze

#endif // BLAZE_IO_WORKER_H
