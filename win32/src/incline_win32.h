#ifndef incline_win32_h
#define incline_win32_h

#ifdef _MSC_VER
#  pragma warning (disable : 4996)
#  define snprintf _snprintf_s
#endif

void sleep(unsigned int seconds);

#endif
