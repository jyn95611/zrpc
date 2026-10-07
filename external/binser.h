#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace binser {

class Counter;
class Serializer;
class Deserializer;

namespace detail {

template <template <class...> class Tmpl, class T>
constexpr bool is_specialization_v = false;

template <template <class...> class Tmpl, class... Args>
constexpr bool is_specialization_v<Tmpl, Tmpl<Args...>> = true;

template <class T>
struct is_std_array : std::false_type {};

template <class T, std::size_t N>
struct is_std_array<std::array<T, N>> : std::true_type {};

template <class T>
constexpr bool is_user_type_v =
    !std::is_arithmetic_v<std::remove_cvref_t<T>> && !std::is_enum_v<std::remove_cvref_t<T>> &&
    !std::is_same_v<std::remove_cvref_t<T>, std::string> &&
    !is_specialization_v<std::vector, std::remove_cvref_t<T>> && !is_std_array<std::remove_cvref_t<T>>::value &&
    !is_specialization_v<std::pair, std::remove_cvref_t<T>> &&
    !is_specialization_v<std::map, std::remove_cvref_t<T>> &&
    !is_specialization_v<std::unordered_map, std::remove_cvref_t<T>> &&
    !is_specialization_v<std::set, std::remove_cvref_t<T>> &&
    !is_specialization_v<std::unordered_set, std::remove_cvref_t<T>> &&
    !is_specialization_v<std::optional, std::remove_cvref_t<T>>;

template <class Ser, class T, class = void>
struct has_serialize : std::false_type {};

template <class Ser, class T>
struct has_serialize<Ser, T, std::void_t<decltype(serialize(std::declval<Ser &>(), std::declval<T &>()))>>
    : std::true_type {};

template <class Derived>
class SerializerBase
{
public:
    bool ok() const { return _ok; }
    void fail() { _ok = false; }

    template <class T>
    void operator()(const T &value)
    {
        if (!ok()) [[unlikely]] {
            return;
        }
        (*this)(const_cast<T &>(value));
    }

    template <class T, class U, class... Rest>
    void operator()(const T &head, const U &next, const Rest &...rest)
    {
        if (!ok()) [[unlikely]] {
            return;
        }
        (*this)(head);
        (*this)(next, rest...);
    }

    template <class T>
        requires std::is_arithmetic_v<T> && (!std::is_const_v<T>)
    void operator()(T &value)
    {
        writeBytes(&value, sizeof(T));
    }

    template <class T>
        requires std::is_enum_v<T> && (!std::is_const_v<T>)
    void operator()(T &value)
    {
        using Underlying = std::underlying_type_t<T>;
        Underlying raw = static_cast<Underlying>(value);
        (*this)(raw);
    }

    void operator()(std::string &value)
    {
        if (!writeCount(value.size())) {
            return;
        }
        if (!value.empty()) {
            writeBytes(value.data(), value.size());
        }
    }

    template <class A, class B>
    void operator()(std::pair<A, B> &value)
    {
        (*this)(value.first, value.second);
    }

    template <class T, std::size_t N>
    void operator()(std::array<T, N> &value)
    {
        if constexpr (std::is_arithmetic_v<T>) {
            if constexpr (N != 0) {
                writeBytes(value.data(), N * sizeof(T));
            }
        } else {
            for (auto &entry : value) {
                (*this)(entry);
                if (!ok()) [[unlikely]] {
                    return;
                }
            }
        }
    }

    template <class T>
        requires is_specialization_v<std::optional, T>
    void operator()(T &value)
    {
        const bool has = value.has_value();
        bool flag = has;
        (*this)(flag);
        if (has) {
            (*this)(*value);
        }
    }

    template <class T>
        requires is_specialization_v<std::map, T> || is_specialization_v<std::unordered_map, T>
    void operator()(T &value)
    {
        if (!writeCount(value.size())) {
            return;
        }
        for (auto &kv : value) {
            (*this)(kv.first, kv.second);
            if (!ok()) [[unlikely]] {
                return;
            }
        }
    }

    template <class T>
        requires is_specialization_v<std::set, T> || is_specialization_v<std::unordered_set, T>
    void operator()(T &value)
    {
        if (!writeCount(value.size())) {
            return;
        }
        for (auto &entry : value) {
            (*this)(entry);
            if (!ok()) [[unlikely]] {
                return;
            }
        }
    }

    template <class T, class Alloc>
    void operator()(std::vector<T, Alloc> &value)
    {
        if (!writeCount(value.size())) {
            return;
        }
        if constexpr (std::is_same_v<T, bool>) {
            for (std::size_t i = 0; i < value.size(); ++i) {
                bool bit = static_cast<bool>(value[i]);
                (*this)(bit);
            }
        } else if constexpr (std::is_arithmetic_v<T>) {
            const std::size_t bytes = value.size() * sizeof(T);
            if (bytes != 0) {
                writeBytes(value.data(), bytes);
            }
        } else {
            for (auto &entry : value) {
                (*this)(entry);
                if (!ok()) [[unlikely]] {
                    return;
                }
            }
        }
    }

    template <class T>
        requires is_user_type_v<T> && (!std::is_const_v<T>)
    void operator()(T &value)
    {
        static_assert(has_serialize<Derived, T>::value,
                      "Define serialize(Ser&, T&) for this type");
        serialize(self(), value);
    }

protected:
    Derived &self() { return static_cast<Derived &>(*this); }

    bool writeCount(std::size_t n)
    {
        if (n > std::numeric_limits<std::uint32_t>::max()) [[unlikely]] {
            fail();
            return false;
        }
        std::uint32_t len = static_cast<std::uint32_t>(n);
        (*this)(len);
        return ok();
    }

    void writeBytes(const void *src, std::size_t n)
    {
        if (!ok()) [[unlikely]] {
            return;
        }
        self().write(src, n);
    }

    bool _ok{true};
};

} // namespace detail

class Counter : public detail::SerializerBase<Counter>
{
public:
    std::size_t size() const { return _size; }

    void write(const void *, std::size_t n) { _size += n; }

private:
    std::size_t _size{0};
};

class Serializer : public detail::SerializerBase<Serializer>
{
public:
    explicit Serializer(char *buf)
        : _p(buf)
    {
    }

    void write(const void *src, std::size_t n)
    {
        if (n == 0) {
            return;
        }
        std::memcpy(_p, src, n);
        _p += n;
    }

private:
    char *_p{nullptr};
};

class Deserializer
{
public:
    Deserializer(const char *buf, std::size_t size)
        : _p(buf),
          _end(buf == nullptr ? nullptr : buf + size)
    {
    }

    bool ok() const { return _ok; }
    void fail() { _ok = false; }
    bool exact() const { return _ok && _p == _end; }

    bool read(void *dst, std::size_t n)
    {
        if (!_ok) [[unlikely]] {
            return false;
        }
        if (n == 0) {
            return true;
        }
        if (remaining() < n) [[unlikely]] {
            fail();
            return false;
        }
        std::memcpy(dst, _p, n);
        _p += n;
        return true;
    }

    template <class T, class U, class... Rest>
    void operator()(T &head, U &next, Rest &...rest)
    {
        if (!_ok) [[unlikely]] {
            return;
        }
        (*this)(head);
        (*this)(next, rest...);
    }

    template <class T>
        requires std::is_arithmetic_v<T> && (!std::is_const_v<T>)
    void operator()(T &value)
    {
        read(&value, sizeof(T));
    }

    template <class T>
        requires std::is_enum_v<T> && (!std::is_const_v<T>)
    void operator()(T &value)
    {
        using Underlying = std::underlying_type_t<T>;
        Underlying raw{};
        (*this)(raw);
        if (_ok) [[likely]] {
            value = static_cast<T>(raw);
        }
    }

    void operator()(std::string &value)
    {
        std::uint32_t len = 0;
        (*this)(len);
        if (!_ok) [[unlikely]] {
            return;
        }
        value.resize(static_cast<std::size_t>(len));
        if (len != 0) {
            read(value.data(), static_cast<std::size_t>(len));
        }
    }

    template <class A, class B>
    void operator()(std::pair<A, B> &value)
    {
        (*this)(value.first, value.second);
    }

    template <class T, std::size_t N>
    void operator()(std::array<T, N> &value)
    {
        if constexpr (std::is_arithmetic_v<T>) {
            if constexpr (N != 0) {
                read(value.data(), N * sizeof(T));
            }
        } else {
            for (auto &entry : value) {
                (*this)(entry);
                if (!_ok) [[unlikely]] {
                    return;
                }
            }
        }
    }

    template <class T>
        requires detail::is_specialization_v<std::optional, T>
    void operator()(T &value)
    {
        bool has = false;
        (*this)(has);
        if (!_ok) [[unlikely]] {
            return;
        }
        if (!has) {
            value.reset();
            return;
        }
        typename T::value_type item{};
        (*this)(item);
        if (_ok) [[likely]] {
            value = std::move(item);
        }
    }

    template <class T>
        requires detail::is_specialization_v<std::map, T> ||
                 detail::is_specialization_v<std::unordered_map, T>
    void operator()(T &value)
    {
        std::uint32_t len = 0;
        (*this)(len);
        if (!_ok) [[unlikely]] {
            return;
        }
        value.clear();
        for (std::uint32_t i = 0; i < len; ++i) {
            typename T::key_type key{};
            typename T::mapped_type mapped{};
            (*this)(key, mapped);
            if (!_ok) [[unlikely]] {
                return;
            }
            value.emplace(std::move(key), std::move(mapped));
        }
    }

    template <class T>
        requires detail::is_specialization_v<std::set, T> ||
                 detail::is_specialization_v<std::unordered_set, T>
    void operator()(T &value)
    {
        std::uint32_t len = 0;
        (*this)(len);
        if (!_ok) [[unlikely]] {
            return;
        }
        value.clear();
        for (std::uint32_t i = 0; i < len; ++i) {
            typename T::value_type item{};
            (*this)(item);
            if (!_ok) [[unlikely]] {
                return;
            }
            value.emplace(std::move(item));
        }
    }

    template <class T, class Alloc>
    void operator()(std::vector<T, Alloc> &value)
    {
        std::uint32_t len = 0;
        (*this)(len);
        if (!_ok) [[unlikely]] {
            return;
        }
        const auto count = static_cast<std::size_t>(len);
        if constexpr (std::is_same_v<T, bool>) {
            value.resize(count);
            for (std::size_t i = 0; i < count; ++i) {
                bool bit = false;
                (*this)(bit);
                if (!_ok) [[unlikely]] {
                    return;
                }
                value[i] = bit;
            }
        } else if constexpr (std::is_arithmetic_v<T>) {
            if (sizeof(T) != 0 && len > std::numeric_limits<std::size_t>::max() / sizeof(T)) [[unlikely]] {
                fail();
                return;
            }
            const std::size_t bytes = count * sizeof(T);
            value.resize(count);
            if (bytes != 0) {
                read(value.data(), bytes);
            }
        } else {
            value.resize(count);
            for (auto &entry : value) {
                (*this)(entry);
                if (!_ok) [[unlikely]] {
                    return;
                }
            }
        }
    }

    template <class T>
        requires detail::is_user_type_v<T> && (!std::is_const_v<T>)
    void operator()(T &value)
    {
        static_assert(detail::has_serialize<Deserializer, T>::value,
                      "Define serialize(Ser&, T&) for this type");
        serialize(*this, value);
    }

private:
    std::size_t remaining() const { return static_cast<std::size_t>(_end - _p); }

    bool _ok{true};
    const char *_p{nullptr};
    const char *_end{nullptr};
};

template <class... Ts>
bool toBinary(std::string &buf, const Ts &...xs)
{
    Counter counter;
    counter(xs...);
    if (!counter.ok()) [[unlikely]] {
        return false;
    }
    buf.resize(counter.size());
    Serializer ser(buf.empty() ? nullptr : buf.data());
    ser(xs...);
    return ser.ok();
}

template <class... Ts>
bool fromBinary(const std::string &buf, Ts &...xs)
{
    Deserializer ser(buf.data(), buf.size());
    ser(xs...);
    return ser.exact();
}

} // namespace binser
