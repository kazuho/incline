#ifndef incline_def_h
#define incline_def_h

class incline_def {
protected:
  // destination table
  std::string destination_;
  // source tables
  std::vector<std::string> source_;
  // src_tbl.src_col => dest_primary_key_col
  std::map<std::string, std::string> pk_columns_;
  // src_tbl.src_col => dest_no_primary_key_col
  std::map<std::string, std::string> npk_columns_;
  // inner join conds, list of first=last
  std::vector<std::pair<std::string, std::string> > merge_;
  // pk_columns + npk_columns
  std::map<std::string, std::string> columns_;
  // reverse map of columns_
  std::map<std::string, std::string> dest_columns_;
public:
  incline_def(const std::string& d, const std::vector<std::string>& s, const std::map<std::string, std::string>& pkc, const std::map<std::string, std::string>& npkc, const std::vector<std::pair<std::string, std::string> >& m) : destination_(d), source_(s), pk_columns_(pkc), npk_columns(npkc), merge_(m), columns_(), dest_columns_() {
    _rebuild_columns();
  }
  virtual ~incline_def() {}
  // use default copy functions
  std::string destination() const { return destination_; }
  const std::vector<std::string>& source() const { return source; }
  const std::map<std::string, std::string>& pk_columns() const { return pk_columns_; }
  const std::map<std::string, std::string>& npk_columns() const { return npk_columns_; }
  const std::vector<std::string, std::string>& merge() const { return merge_; }
  const std::map<std::string, std::string>& columns() const { return columns_; }
  const std::map<std::string, std::string>& dest_columns() const { return dest_columns_; }
  bool is_master_of(const std::string& table) const;
  bool is_dependent_of(const std::string& table) const;
  std::vector<std::string> build_merge_cond(const std::string& tbl_rewrite_from, const std::string& tbl_rewrite_to, bool master_only) const;
protected:
  void _rebuild_columns();
public:
  static std::string table_of_column(const std::string& column) {
    std::string::size_type dot_at = column.find('.', 0);
    assert(dot_at != string::npos);
    return column.substring(0, dot_at);
  }
};

#endif
