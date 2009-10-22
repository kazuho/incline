#ifndef incline_util_h
#define incline_util_h

#include <cstring>
#include <string>
#include <vector>
#include <map>

struct incline_util {
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
  template <typename T, typename V> static std::string join(const T& delimiter, const V& list) {
    return join(delimiter, list.begin(), list.end());
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
  
  class filter_func_t {
  protected:
    std::string (*f_)(const std::string& s);
  public:
    filter_func_t(std::string (*f)(const std::string&)) : f_(f) {}
    virtual ~filter_func_t();
    virtual std::string operator()(const std::string& s) const {
      return f_ != NULL ? (*f_)(s) : s;
    }
  };
  
  class rewrite_prefix : public filter_func_t {
  protected:
    std::string orig_, repl_;
  public:
    rewrite_prefix(const std::string& orig, const std::string& repl) : filter_func_t(NULL), orig_(orig), repl_(repl) {}
    virtual ~rewrite_prefix() {}
    virtual std::string operator()(const std::string& s) const;
  };
  
  static std::vector<std::string> filter(const filter_func_t& func, const std::vector<std::string>& list);
  static std::map<std::string, std::string> filter(const filter_func_t& keyfunc, const filter_func_t& valuefunc, const std::map<std::string, std::string>& map);
  static std::string filter(const char* fmt, int idx, size_t n, ...);
  static std::vector<std::string> filter(const char* fmt, const std::vector<std::string>& list);
  static std::vector<std::string> filter(const char* fmt, const std::map<std::string, std::string>& map);
  
  static bool is_one_of(const char* dnt, const char* cmp) {
    for (; *dnt != '\0'; dnt += strlen(dnt) + 1) {
      if (strcmp(dnt, cmp) == 0) {
	return true;
      }
    }
    return false;
  }
  static bool is_one_of(const char* dnt, const std::string& s) {
    return is_one_of(dnt, s.c_str());
  }
};

#endif
