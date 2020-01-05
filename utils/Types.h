//
// Created by jason on 2019/12/24.
//

#ifndef YCRT_UTILS_TYPES_H_
#define YCRT_UTILS_TYPES_H_

#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))

#define DISALLOW_COPY_AND_ASSIGN(TypeName)          \
TypeName(const TypeName&) = delete;                 \
TypeName& operator=(const TypeName&) = delete

#define DISALLOW_COPY_MOVE_AND_ASSIGN(TypeName)     \
TypeName(const TypeName&) = delete;                 \
TypeName& operator=(const TypeName&) = delete;      \
TypeName(TypeName&&) = delete;                      \
TypeName& operator=(const TypeName&&) = delete

#define DEFAULT_COPY_AND_ASSIGN(TypeName)           \
TypeName(const TypeName&) = default;                \
TypeName& operator=(const TypeName&) = default

#define DEFAULT_MOVE_AND_ASSIGN(TypeName)           \
TypeName(TypeName&&) noexcept = default;            \
TypeName& operator=(TypeName&&) noexcept = default

#define DEFAULT_COPY_MOVE_AND_ASSIGN(TypeName)      \
TypeName(const TypeName&) = default;                \
TypeName& operator=(const TypeName&) = default;     \
TypeName(TypeName&&) noexcept = default;            \
TypeName& operator=(TypeName&&) noexcept = default

#endif //YCRT_UTILS_TYPES_H_
