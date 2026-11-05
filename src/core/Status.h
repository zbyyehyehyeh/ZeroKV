#pragma once
#include <string>

class Status {
public:
    enum Code { kOk = 0, kNotFound = 1, kCorruption = 2, kIOError = 3 };

    Status() : code_(kOk) {}
    static Status OK() { return Status(); }
    static Status NotFound(const std::string& msg = "") { return Status(kNotFound, msg); }
    static Status Corruption(const std::string& msg = "") { return Status(kCorruption, msg); }
    static Status IOError(const std::string& msg = "") { return Status(kIOError, msg); }

    bool ok() const { return code_ == kOk; }
    bool IsNotFound() const { return code_ == kNotFound; }
    bool IsCorruption() const { return code_ == kCorruption; }
    
    std::string ToString() const {
        if (ok()) return "OK";
        const char* type = nullptr;
        switch (code_) {
            case kNotFound: type = "NotFound"; break;
            case kCorruption: type = "Corruption"; break;
            case kIOError: type = "IOError"; break;
            default: type = "Unknown"; break;
        }
        return std::string(type) + ": " + msg_;
    }

private:
    Status(Code code, const std::string& msg) : code_(code), msg_(msg) {}
    Code code_;
    std::string msg_;
};