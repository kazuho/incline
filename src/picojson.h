#ifndef picojson_h
#define picojson_h

#include <string>
#include <vector>
#include <map>

namespace picojson {
  
  enum {
    undefined_type,
    null_type,
    boolean_type,
    integer_type,
    string_type,
    array_type,
    object_type
  };
  
  struct undefined {};
  
  struct null {};
  
  class value {
  public:
    typedef std::vector<value> value_array;
    typedef std::map<std::string, value> value_object;
  protected:
    int type_;
    union {
      bool boolean_;
      int integer_;
      std::string* string_;
      value_array* array_;
      value_object* object_;
    };
  public:
    value(int type);
    ~value();
    value(const value& x);
    value& operator=(const value& x) {
      if (this != &x) {
	this->~value();
	new (this) value(x);
      }
      return *this;
    }
    template <typename T> bool is() const;
    template <typename T> const T& get() const;
    template <typename T> T& get();
  };
  
  inline value::value(int type) : type_(type) {
    switch (type) {
#define INIT(p, v) case p##type: p = v; break
      INIT(boolean_, false);
      INIT(integer_, 0);
      INIT(string_, new std::string());
      INIT(array_, new value_array());
      INIT(object_, new value_object());
#undef INIT
    default: break;
    }
  }
  
  typedef value::value_array array;
  typedef value::value_object object;
  
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
      INIT(integer_, x.integer_);
      INIT(string_, new std::string(*x.string_));
      INIT(array_, new value_array(*x.array_));
      INIT(object_, new value_object(*x.object_));
#undef INIT
    default: break;
    }
  }
  
#define IS(ctype, jtype)			     \
  template <> inline bool value::is<ctype>() const { \
    return type_ == jtype##_type;		     \
  }
  IS(undefined, undefined)
  IS(null, null)
  IS(bool, boolean)
  IS(int, integer)
  IS(std::string, string)
  IS(array, array)
  IS(object, object)
#undef IS
#define GET(ctype, jtype)				      \
  template <> inline const ctype& value::get<ctype>() const { \
    return jtype##_;					      \
  }
  GET(bool, boolean)
  GET(int, integer)
  GET(std::string, *string)
  GET(array, *array)
  GET(object, *object)
#undef GET
  
}

#endif
