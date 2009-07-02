#ifndef picojson_h
#define picojson_h

#include <cassert>
#include <cstring>
#include <string>
#include <vector>
#include <map>

namespace picojson {
  
  enum {
    undefined_type,
    null_type,
    boolean_type,
    number_type,
    string_type,
    array_type,
    object_type
  };
  
  struct undefined {};
  
  struct null {};
  
  class value {
  public:
    typedef std::vector<value> array;
    typedef std::map<std::string, value> object;
  protected:
    int type_;
    union {
      bool boolean_;
      double number_;
      std::string* string_;
      array* array_;
      object* object_;
    };
  public:
    value(int type = undefined_type);
    ~value();
    value(const value& x);
    value& operator=(const value& x);
    template <typename T> bool is() const;
    template <typename T> const T& get() const;
    template <typename T> T& get();
    operator bool() const;
    const value& get(const std::string& key) const;
    std::string to_str() const;
  };
  
  typedef value::array array;
  typedef value::object object;
  
  inline value::value(int type) : type_(type) {
    switch (type) {
#define INIT(p, v) case p##type: p = v; break
      INIT(boolean_, false);
      INIT(number_, 0.0);
      INIT(string_, new std::string());
      INIT(array_, new array());
      INIT(object_, new object());
#undef INIT
    default: break;
    }
  }
  
  inline value::~value() {
    switch (type_) {
#define DEINIT(p) case p##type: delete p; break
      DEINIT(string_);
      DEINIT(array_);
      DEINIT(object_);
#undef DEINIT
    default: break;
    }
  }
  
  inline value::value(const value& x) : type_(x.type_) {
    switch (type_) {
#define INIT(p, v) case p##type: p = v; break
      INIT(boolean_, x.boolean_);
      INIT(number_, x.number_);
      INIT(string_, new std::string(*x.string_));
      INIT(array_, new array(*x.array_));
      INIT(object_, new object(*x.object_));
#undef INIT
    default: break;
    }
  }
  
  inline value& value::operator=(const value& x) {
    if (this != &x) {
      this->~value();
      new (this) value(x);
    }
    return *this;
  }
  
#define IS(ctype, jtype)			     \
  template <> inline bool value::is<ctype>() const { \
    return type_ == jtype##_type;		     \
  }
  IS(undefined, undefined)
  IS(null, null)
  IS(bool, boolean)
  IS(int, number)
  IS(double, number)
  IS(std::string, string)
  IS(array, array)
  IS(object, object)
#undef IS
  
#define GET(ctype, var)					      \
  template <> inline const ctype& value::get<ctype>() const { \
    return var;						      \
  }							      \
  template <> inline ctype& value::get<ctype>() {	      \
    return var;						      \
  }
  GET(bool, boolean_)
  GET(double, number_)
  GET(std::string, *string_)
  GET(array, *array_)
  GET(object, *object_)
#undef GET
  
  inline value::operator bool() const {
    switch (type_) {
    case undefined_type:
    case null_type:
      return false;
    case boolean_type:
      return boolean_;
    case number_type:
      return number_;
    case string_type:
      return ! string_->empty();
    default:
      return true;
    }
  }
  
  inline const value& value::get(const std::string& key) const {
    static value s_undefined(undefined_type);
    assert(is<object>());
    object::const_iterator i = object_->find(key);
    return i != object_->end() ? i->second : s_undefined;
  }
  
  inline std::string value::to_str() const {
    switch (type_) {
    case undefined_type: return "undefined";
    case null_type:      return "null";
    case boolean_type:   return boolean_ ? "true" : "false";
    case number_type:    {
      char buf[256];
      snprintf(buf, sizeof(buf), "%f", number_);
      return buf;
    }
    case string_type:    return *string_;
    case array_type:     return "array";
    case object_type:    return "object";
    default:             assert(0);
    }
  }
  
  template <typename Iter> class input {
  protected:
    Iter cur_, end_;
    int last_ch_;
    bool ungot_;
    int line_;
  public:
    input(const Iter& first, const Iter& last) : cur_(first), end_(last), last_ch_(-1), ungot_(false), line_(1) {}
    bool eof() const { return cur_ == end_ && ! ungot_; }
    int getc() {
      if (ungot_) {
	ungot_ = false;
	return last_ch_;
      }
      if (cur_ == end_) {
	return -1;
      }
      if (last_ch_ == '\n') {
	line_++;
      }
      last_ch_ = *cur_++ & 0xff;
      return last_ch_;
    }
    void ungetc() {
      if (last_ch_ != -1) {
	assert(! ungot_);
	ungot_ = true;
      }
    }
    Iter cur() const { return cur_; }
    int line() const { return line_; }
    void skip_ws() {
      while (! eof() && isspace(getc()))
	;
      ungetc();
    }
    enum {
      error,
      negative,
      positive
    };
    int match(const std::string& pattern) {
      skip_ws();
      std::string::const_iterator pi(pattern.begin());
      for (; pi != pattern.end(); ++pi) {
	if (eof()) {
	  break;
	} else if (getc() != *pi) {
	  ungetc();
	  break;
	}
      }
      if (pi == pattern.end()) {
	return positive;
      } else if (pi == pattern.begin()) {
	return negative;
      } else {
	return error;
      }
    }
  };
  
  template<typename Iter> static bool _parse_string(value& out, input<Iter>& in) {
    // gcc 4.1 cannot compile if the below two lines are merged into one :-(
    out = value(string_type);
    std::string& s = out.get<std::string>();
    while (! in.eof()) {
      int ch = in.getc();
      if (ch == '"') {
	return true;
      } else if (ch == '\\') {
	if (in.eof()) {
	  return false;
	}
	ch = in.getc();
      }
      s.push_back(ch);
    }
    return false;
  }
  
  template <typename Iter> static bool _parse_array(value& out, input<Iter>& in) {
    out = value(array_type);
    array& a = out.get<array>();
    do {
      a.push_back(value());
      if (! _parse(a.back(), in)) {
	return false;
      }
    } while (in.match(",") == input<Iter>::positive);
    return in.match("]") == input<Iter>::positive;
  }
  
  template <typename Iter> static bool _parse_object(value& out, input<Iter>& in) {
    out = value(object_type);
    object &o = out.get<object>();
    do {
      value key, val;
      if (in.match("\"") == input<Iter>::positive
	  && _parse_string(key, in) == input<Iter>::positive
	  && in.match(":") == input<Iter>::positive
	  && _parse(val, in) == input<Iter>::positive) {
	o[key.to_str()] = val;
      }
    } while (in.match(",") == input<Iter>::positive);
    return in.match("}") == input<Iter>::positive;
  }
  
  template <typename Iter> static bool _parse(value& out, input<Iter>& in) {
    int ret = input<Iter>::negative;
#define IS(p)						\
    (ret == input<Iter>::negative			\
     && (ret = in.match(p)) == input<Iter>::positive)
    if (IS("undefined")) {
      out = value(undefined_type);
    } else if (IS("null")) {
      out = value(null_type);
    } else if (IS("false")) {
      out = value(boolean_type);
    } else if (IS("true")) {
      out = value(boolean_type);
      out.get<bool>() = true;
    } else if (IS("\"")) {
      if (! _parse_string(out, in)) {
	return true;
      }
    } else if (IS("[")) {
      if (! _parse_array(out, in)) {
	return true;
      }
    } else if (IS("{")) {
      if (_parse_object(out, in)) {
	return true;
      }
    }
    // TODO number support
#undef IS
    return ret == input<Iter>::positive;
  }
  
  template <typename Iter> static std::string parse(value& out, Iter& pos, const Iter& last) {
    // setup
    input<Iter> in(pos, last);
    std::string err;
    // do
    if (! _parse(out, in)) {
      char buf[64];
      sprintf(buf, "syntax error at line %d near: ", in.line());
      err = buf;
      while (! in.eof()) {
	int ch = in.getc();
	if (ch == '\n') {
	  break;
	}
	err += ch;
      }
    }
    pos = in.cur();
    return err;
  }
  
}

#endif
#ifdef TEST_PICOJSON

using namespace std;
  
static void plan(int num)
{
  printf("1..%d\n", num);
}

static void ok(bool b, const char* name = "")
{
  static int n = 1;
  printf("%s %d - %s\n", b ? "ok" : "ng", n++, name);
}

template <typename T> void is(const T& x, const T& y, const char* name = "")
{
  if (x == y) {
    ok(true, name);
  } else {
    ok(false, name);
  }
}

int main(void)
{
  plan(13);
  
  
#define TEST(in, type, cmp) {						\
    picojson::value v;							\
    const char* s = in;							\
    string err = picojson::parse(v, s, s + strlen(s));			\
    ok(err.empty(), in " no error");					\
    ok(v.is<type>(), in " check type");					\
    is(v.get<type>(), cmp, in " correct output");			\
    is(*s, '\0', in " read to eof");					\
  }
  
  TEST("false", bool, false);
  TEST("true", bool, true);
  TEST("\"hello\"", string, string("hello"));
  
  {
    picojson::value v;
    const char *s = "falsoooo";
    string err = picojson::parse(v, s, s + strlen(s));
    is(err, string("syntax error at line 1 near: oooo"), "error message");
  }
  
#undef TEST
  
  return 0;
}

#endif
