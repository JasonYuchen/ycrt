//
// Created by jason on 2019/12/21.
//

#ifndef YCRT_UTILS_UTILS_H_
#define YCRT_UTILS_UTILS_H_

#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))

#define DEFAULT_COPY_AND_ASSIGN(TypeName)           \
TypeName(const TypeName&) = default;                \
TypeName& operator=(const TypeName&) = default

#define DISALLOW_COPY_AND_ASSIGN(TypeName)          \
TypeName(const TypeName&) = delete;                 \
TypeName& operator=(const TypeName&) = delete

#define DISALLOW_COPY_MOVE_AND_ASSIGN(TypeName)     \
TypeName(const TypeName&) = delete;                 \
TypeName& operator=(const TypeName&) = delete;      \
TypeName(TypeName&&) = delete;                      \
TypeName& operator=(const TypeName&&) = delete

namespace ycrt
{

namespace utils
{



} // namespace utils

} // namespace ycrt

#endif //YCRT_UTILS_UTILS_H_
