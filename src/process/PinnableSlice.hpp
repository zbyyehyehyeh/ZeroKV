#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <memory>

class Slice
{
public:
    Slice() : data_(""), size_(0) {}
    Slice(const char *d, size_t n) : data_(d), size_(n) {}
    Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}
    const char *data() const { return data_; }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

protected:
    const char *data_;
    size_t size_;
};

class PinnableSlice : public Slice
{
public:
    PinnableSlice() = default;

    void assign(const char *data, size_t size)
    {
        Reset();
        buffer_.assign(data, size);
        data_ = buffer_.data(); 
        size_ = size;
        pinned_ = false;
    }

    void pin_external(const char *data, size_t size, std::shared_ptr<void> cache_handle)
    {
        Reset();
        data_ = data;
        size_ = size;
        cache_handle_ = std::move(cache_handle);
        pinned_ = true;
    }

    void pin_memtable(const char *data, size_t size)
    {
        assign(data, size); 
    }

    void Reset()
    {
        if (pinned_)
        {
            cache_handle_.reset();
        }
        else
        {
            buffer_.clear();
        }
        data_ = "";
        size_ = 0;
        pinned_ = false;
    }

    ~PinnableSlice() { Reset(); }

    PinnableSlice(const PinnableSlice &) = delete;
    PinnableSlice &operator=(const PinnableSlice &) = delete;

    PinnableSlice(PinnableSlice &&other) noexcept { MoveFrom(std::move(other)); }
    PinnableSlice &operator=(PinnableSlice &&other) noexcept
    {
        if (this != &other)
        {
            Reset();
            MoveFrom(std::move(other));
        }
        return *this;
    }

private:
    void MoveFrom(PinnableSlice &&other)
    {
        pinned_ = other.pinned_;
        size_ = other.size_;
        
        if (pinned_)
        {
            data_ = other.data_;
            cache_handle_ = std::move(other.cache_handle_);
        }
        else
        {
            buffer_ = std::move(other.buffer_);
            data_ = buffer_.empty() ? "" : buffer_.data(); 
        }
        
        other.data_ = "";
        other.size_ = 0;
        other.pinned_ = false;
        other.cache_handle_.reset(); // ✅ 显式重置源对象的 handle
    }

    std::string buffer_;
    std::shared_ptr<void> cache_handle_;
    bool pinned_ = false;
};