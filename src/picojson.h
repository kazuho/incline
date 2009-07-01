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
    value(int type);
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
    static value s_undefined(undefined_type); // move inside a template?
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
}

#endif
