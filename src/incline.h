#ifndef incline_h
#define incline_h

#include <string>
#include <vector>

struct incline {
  template <typename T, typename Iter> static std::string join(const T& delimiter, const Iter& first, const Iter& last) {
    std::string r;
    for (Iter i = first; i != last; ++i) {
      if (i != first) {
	r += delimiter;
      }
      r += *i;
    }
    return r;
  }
  template <typename T> static std::vector<T> vectorize(const T& t) {
    std::vector<T> r;
    r.push_back(t);
    return r;
  }
  template <typename T1, typename T2> static void push_back(T1& target, const T2& source) {
    for (typename T2::const_iterator si = source.begin();
	 si != source.end();
	 ++si) {
      target.push_back(*si);
    }
  }
  template <typename T1, typename T2> static void push_back(T1& target, const T2& source, const std::string& prefix) {
    for (typename T2::const_iterator si = source.begin();
	 si != source.end();
	 ++si) {
      target.push_back(prefix + *si);
    }
  }
};

#endif
