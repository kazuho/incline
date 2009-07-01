#ifndef picojson_h
#define picojson_h

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
  
  class value {
  protected:
    int type_;
    union {
      bool boolean_;
      int integer_;
      std::string* string_;
      std::vector<value>* array_;
      std::map<std::string, value> object_;
    };
  public:
    value(int type) : type_(type) {
      switch (type) {
#define INIT(p, v) case p##type: p = v; break
	INIT(boolean_, false);
	INIT(integer_, 0);
	INIT(string_, new string());
	INIT(array_, new std::vector<value>());
	INIT(object_, new std::map<std::string, value>());
#undef INIT
      default: break;
      }
    }
    ~value() {
      switch (type) {
#define DEINIT(p) case p##type: delete p; break
	DEINIT(string_);
	DEINIT(array_);
	DEINIT(object_);
#undef DEINIT
      default: break;
      }
    }
    value(const value& x) : type_(type) {
      switch (type) {
#define INIT(p, v) case p##type: p = v; break
	INIT(boolean_, x.boolean_);
	INIT(integer_, x.integer_);
	INIT(string_, new string(x.string_));
	INIT(array_, new std::vector<value>(x.array_));
	INIT(object_, new std::map<std::stirng, value>(x.object_));
#undef INIT
      default: break;
      }
    }
    value& operator=(const value& x) {
      if (this != &x) {
	~value();
	new (this) value(x);
      }
      return *this;
    }
    template <typename T> is() const;
    template <typename T> const T& get() const;
    template <typename T> T& get();
  };
  
}

#endif
