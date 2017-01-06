#pragma once

#include <memory>
#include <mutex>

namespace clickhouse {

template <typename T>
T* Singleton() {
    static std::once_flag flag;
    static std::unique_ptr<T> instance;

    std::call_once(flag, []
        {
            instance.reset(new T);
        }
    );

    return instance.get();
}

}
