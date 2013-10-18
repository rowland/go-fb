#ifndef FB_H
#define FB_H

#include <ibase.h>

#define	FB_ALIGN(n, b)	((n + b - 1) & ~(b - 1))
#define SHORT_SIZE sizeof(short)

char* trans_parseopts(char *opt, long *tpb_len);
XSQLDA* sqlda_alloc(long cols);
long calculate_buffsize(XSQLDA *sqlda);
XSQLVAR* sqlda_sqlvar(XSQLDA* sqlda, ISC_SHORT col);

/* InterBase varchar structure */
typedef struct
{
	short vary_length;
	char  vary_string[1];
} VARY;

#endif
