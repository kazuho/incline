#ifndef listctx_h
#define listctx_h

#include <cstddef>

template <typename T, size_t capacity> class listctx_t {
public:
  class const_iterator {
  protected:
    T* const* t_;
  public:
    const_iterator(T* const* t) : t_(t) {}
    const T& operator*() { return **t_; }
    const_iterator& operator++() { ++t_; return *this; }
    const_iterator operator++(int) { const_iterator r(t_); ++t_; return r; }
#define listctx_def_op(op) bool operator op (const const_iterator& x) { return t_ op x.t_; }
    listctx_def_op(==)
    listctx_def_op(!=)
    listctx_def_op(<)
    listctx_def_op(<=)
    listctx_def_op(>)
    listctx_def_op(>=)
#undef listctx_def_op
  };
protected:
  T* t_[capacity];
  template <typename C> void _copy(const C& x) {
    size_t idx = 0;
    for (typename C::const_iterator it = x.begin();
	 it != x.end() && idx != capacity;
	 ++it, ++idx) {
      *t_[idx] = *it;
    }
  }
public:
  listctx_t& operator=(const listctx_t<T, capacity>& x) {
    if (this != &x) {
      _copy(x);
    }
    return *this;
  }
  template <typename C> listctx_t& operator=(const C& x) {
    _copy(x);
    return *this;
  }
  listctx_t& set(T* t, size_t idx) {
    t_[idx] = t;
    return *this;
  }
  const_iterator begin() const { return t_; }
  const_iterator end() const { return t_ + capacity; }
};

#define listctx_set(n) r.set(&t##n, n - 1)

template<typename T> listctx_t<T, 1> listctx(T& t1)
{
  listctx_t<T, 1> r;
  listctx_set(1);
  return r;
}

template<typename T> listctx_t<T, 2> listctx(T& t1, T& t2)
{
  listctx_t<T, 2> r;
  listctx_set(1);
  listctx_set(2);
  return r;
}

template<typename T> listctx_t<T, 3> listctx(T& t1, T& t2, T& t3)
{
  listctx_t<T, 3> r;
  listctx_set(1);
  listctx_set(2);
  listctx_set(3);
  return r;
}

template<typename T> listctx_t<T, 4> listctx(T& t1, T& t2, T& t3, T& t4)
{
  listctx_t<T, 4> r;
  listctx_set(1);
  listctx_set(2);
  listctx_set(3);
  listctx_set(4);
  return r;
}

template<typename T> listctx_t<T, 5> listctx(T& t1, T& t2, T& t3, T& t4, T& t5)
{
  listctx_t<T, 5> r;
  listctx_set(1);
  listctx_set(2);
  listctx_set(3);
  listctx_set(4);
  listctx_set(5);
  return r;
}

template<typename T> listctx_t<T, 6> listctx(T& t1, T& t2, T& t3, T& t4, T& t5, T& t6)
{
  listctx_t<T, 6> r;
  listctx_set(1);
  listctx_set(2);
  listctx_set(3);
  listctx_set(4);
  listctx_set(5);
  listctx_set(6);
  return r;
}

template<typename T> listctx_t<T, 7> listctx(T& t1, T& t2, T& t3, T& t4, T& t5, T& t6, T& t7)
{
  listctx_t<T, 7> r;
  listctx_set(1);
  listctx_set(2);
  listctx_set(3);
  listctx_set(4);
  listctx_set(5);
  listctx_set(6);
  listctx_set(7);
  return r;
}

#undef listctx_set

#ifdef listctx_test

#include <vector>
#include <stdio.h>

int main(void)
{
  std::vector<int> v;
  v.push_back(1);
  v.push_back(2);
  v.push_back(3);
  
  int one = 0, two = 0, three = 0;
  
  listctx(one, two, three) = v;
  
  printf("one=%d,two=%d,three=%d\n", one, two, three);
  
  int v1 = 1, v2 = 2;
  one = two = 0;
  listctx(one, two, three) = listctx(v1, v2);
  printf("one=%d,two=%d,three=%d\n", one, two, three);
  
  return 0;
}

#endif

#endif
