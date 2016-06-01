#include <ibase.h>
#include <stdlib.h>
#include <string.h>
#include "_cgo_export.h"
#include "fb.h"

typedef struct trans_opts
{
	const char *option1;
	const char *option2;
	char  optval;
	short position;
	struct trans_opts *sub_opts;
} trans_opts;

#define ALLOCA_N(type,n) (type*)alloca(sizeof(type)*(n))
#define	UPPER(c)	(((c) >= 'a' && (c)<= 'z') ? (c) - 'a' + 'A' : (c))

#define	TPBBUFF_ALLOC	64

#define	CMND_DELIMIT	" \t\n\r\f"
#define	LIST_DELIMIT	", \t\n\r\f"
#define	META_NAME_MAX	31

#define	RESV_TABLEEND	"FOR"
#define	RESV_SHARED	"SHARED"
#define	RESV_PROTECTD	"PROTECTED"
#define	RESV_READ	"READ"
#define	RESV_WRITE	"WRITE"
#define	RESV_CONTINUE	','

static char isc_tpb_0[] = {
    isc_tpb_version1,		isc_tpb_write,
    isc_tpb_concurrency,	isc_tpb_nowait
};

static trans_opts	rcom_opt_S[] =
{
	{"NO",			"RECORD_VERSION",	isc_tpb_no_rec_version,	-1,	0},
	{"RECORD_VERSION",	0,			isc_tpb_rec_version,	-1,	0},
	{"*",			0,			isc_tpb_no_rec_version,	-1,	0},
	{0,			0,			0,			0,	0}
};

static trans_opts	read_opt_S[] =
{
	{"WRITE",	0,	isc_tpb_write,		1,	0},
	{"ONLY",		0,	isc_tpb_read,		1,	0},
	{"COMMITTED",	0,	isc_tpb_read_committed,	2,	rcom_opt_S},
	{0,		0,	0,			0,	0}
};

static trans_opts	snap_opt_S[] =
{
	{"TABLE",	"STABILITY",	isc_tpb_consistency,	2,	0},
	{"*",		0,		isc_tpb_concurrency,	2,	0},
	{0,			0,	0,			0,	0}
};

static trans_opts	isol_opt_S[] =
{
	{"SNAPSHOT",	0,		0,			0,	snap_opt_S},
	{"READ",		"COMMITTED",	isc_tpb_read_committed,	2,	rcom_opt_S},
	{0,		0,		0,			0,	0}
};

static trans_opts	trans_opt_S[] =
{
	{"READ",		0,		0,		0,	read_opt_S},
	{"WAIT",		0,		isc_tpb_wait,	3,	0},
	{"NO",		"WAIT",		isc_tpb_nowait,	3,	0},
	{"ISOLATION",	"LEVEL",	0,		0,	isol_opt_S},
	{"SNAPSHOT",	0,		0,		0,	snap_opt_S},
	{"RESERVING",	0,		-1,		0,	0},
	{0,		0,		0,		0,	0}
};

char* trans_parseopts(char *opt, long *tpb_len)
{
	char *s, *trans;
	long used;
	long size;
	char *tpb;
	trans_opts *curr_p;
	trans_opts *target_p;
	char *check1_p;
	char *check2_p;
	int count;
	int next_c;
	char check_f[4];
	char *resv_p;
	char *resend_p;
	char *tblend_p = 0;
	long tbl_len;
	long res_first;
	int res_count;
	long ofs;
	char sp_prm;
	char rw_prm;
	int cont_f;
	char *desc = 0;

	 // Initialize 
	s = opt;
	trans = ALLOCA_N(char, strlen(s)+1);
	strcpy(trans, s);
	s = trans;
	while (*s) {
		*s = UPPER(*s);
		s++;
	}

	used = 0;
	size = 0;
	tpb = NULL;
	memset((void *)check_f, 0, sizeof(check_f));

	 // Set the default transaction option 
	tpb = (char*)malloc(TPBBUFF_ALLOC);
	size = TPBBUFF_ALLOC;
	memcpy((void*)tpb, (void*)isc_tpb_0, sizeof(isc_tpb_0));
	used = sizeof(isc_tpb_0);

	 // Analize the transaction option strings 
	curr_p = trans_opt_S;
	check1_p = strtok(trans, CMND_DELIMIT);
	if (check1_p) {
		check2_p = strtok(0, CMND_DELIMIT);
	} else {
		check2_p = 0;
	}
	while (curr_p) {
		target_p = 0;
		next_c = 0;
		for (count = 0; curr_p[count].option1; count++) {
			if (!strcmp(curr_p[count].option1, "*")) {
				target_p = &curr_p[count];
				break;
			} else if (check1_p && !strcmp(check1_p, curr_p[count].option1)) {
				if (!curr_p[count].option2) {
					next_c = 1;
					target_p = &curr_p[count];
					break;
				} else if (check2_p && !strcmp(check2_p, curr_p[count].option2)) {
					next_c = 2;
					target_p = &curr_p[count];
					break;
				}
			}
		}

		if (!target_p) {
			desc = "Illegal transaction option was specified";
			goto error;
		}

		 // Set the transaction option 
		if (target_p->optval > '\0') {
			if (target_p->position > 0) {
				if (check_f[target_p->position]) {
					desc = "Duplicate transaction option was specified";
					goto error;
				}
				tpb[target_p->position] = target_p->optval;
				check_f[target_p->position] = 1;
			} else {
				if (used + 1 > size) {
					tpb = (char *)realloc(tpb, size + TPBBUFF_ALLOC);
					size += TPBBUFF_ALLOC;
				}
				tpb[used] = target_p->optval;
				used++;
			}
		} else if (target_p->optval) {		// RESERVING ... FOR
			if (check_f[0]) {
				desc = "Duplicate transaction option was specified";
				goto error;
			}
			resv_p = check2_p;
			if (!resv_p || !strcmp(resv_p, RESV_TABLEEND)) {
				desc = "RESERVING needs table name list";
				goto error;
			}
			while (resv_p) {
				res_first = used;
				res_count = 0;
				resend_p = strtok(0, CMND_DELIMIT);
				while (resend_p) {
					if (!strcmp(resend_p, RESV_TABLEEND)) {
						break;
					}
					resend_p = strtok(0, CMND_DELIMIT);
				}

				if (!resend_p) {
					desc = "Illegal transaction option was specified";
					goto error;
				}

				while (resv_p < resend_p) {
					if (*resv_p == '\0' || (ofs = strspn(resv_p, LIST_DELIMIT)) < 0) {
						resv_p++;
					} else {
						resv_p = &resv_p[ofs];
						tblend_p = strpbrk(resv_p, LIST_DELIMIT);
						if (tblend_p) {
							tbl_len = tblend_p - resv_p;
						} else {
							tbl_len = strlen(resv_p);
						}
						if (tbl_len > META_NAME_MAX) {
							desc = "Illegal table name was specified";
							goto error;
						}

						if (tbl_len > 0) {
							if (used + tbl_len + 3 > size) {
								tpb = (char*)realloc(tpb, size+TPBBUFF_ALLOC);
								size += TPBBUFF_ALLOC;
							}
							tpb[used+1] = (char)tbl_len;
							memcpy((void *)&tpb[used+2],resv_p, tbl_len);
							used += tbl_len + 3;
							res_count++;
						}
						resv_p += tbl_len;
					}
				}

				resv_p = strtok(0, CMND_DELIMIT);
				if (resv_p && !strcmp(resv_p, RESV_SHARED)) {
					sp_prm = isc_tpb_shared;
				} else if (resv_p && !strcmp(resv_p, RESV_PROTECTD)) {
					sp_prm = isc_tpb_protected;
				} else {
					desc = "RESERVING needs {SHARED|PROTECTED} {READ|WRITE}";
					goto error;
				}

				cont_f = 0;
				resv_p = strtok(0, CMND_DELIMIT);
				if (resv_p) {
					if (resv_p[strlen(resv_p)-1] == RESV_CONTINUE) {
						cont_f = 1;
						resv_p[strlen(resv_p)-1] = '\0';
					} else {
						tblend_p = strpbrk(resv_p, LIST_DELIMIT);
						if (tblend_p) {
							cont_f = 2;
							*tblend_p = '\0';
						}
					}
				}

				if (resv_p && !strcmp(resv_p, RESV_READ)) {
					rw_prm = isc_tpb_lock_read;
				} else if (resv_p && !strcmp(resv_p, RESV_WRITE)) {
					rw_prm = isc_tpb_lock_write;
				} else {
					desc = "RESERVING needs {SHARED|PROTECTED} {READ|WRITE}";
					goto error;
				}

				ofs = res_first;
				for (count = 0; count < res_count; count++) {
					tpb[ofs++] = rw_prm;
					ofs += tpb[ofs] + 1;
					tpb[ofs++] = sp_prm;
				}

				if (cont_f == 1) {
					resv_p = strtok(0, CMND_DELIMIT);
					if (!resv_p) {
						desc = "Unexpected end of command";
						goto error;
					}
				}
				if (cont_f == 2) {
					resv_p = tblend_p + 1;
				} else {
					resv_p = strtok(0, CMND_DELIMIT);
					if (resv_p) {
						if ((int)strlen(resv_p) == 1 && resv_p[0] == RESV_CONTINUE) {
							resv_p = strtok(0, CMND_DELIMIT);
							if (!resv_p) {
								desc = "Unexpected end of command";
								goto error;
							}
						} else if (resv_p[0] == RESV_CONTINUE) {
							resv_p++;
						} else {
							next_c = 1;
							check2_p = resv_p;
							resv_p = 0;
						}
					} else {
						next_c = 0;
						check1_p = check2_p = 0;
					}
				}
			}

			check_f[0] = 1;
		}

		 // Set the next check list 
		curr_p = target_p->sub_opts;

		for (count = 0; count < next_c; count++) {
			check1_p = check2_p;
			if (check2_p) {
				check2_p = strtok(0, CMND_DELIMIT);
			}
		}

		if (check1_p && !curr_p) {
			curr_p = trans_opt_S;
		}
	}

	 // Set the results 
	*tpb_len = used;
	return tpb;

error:
	free(tpb);
	*tpb_len = -1;
	return desc;
}

XSQLDA* sqlda_alloc(long cols)
{
	XSQLDA *sqlda;

	sqlda = (XSQLDA*)malloc(XSQLDA_LENGTH(cols));
	sqlda->version = SQLDA_VERSION1;
	sqlda->sqln = cols;
	sqlda->sqld = 0;
	return sqlda;
}

long calculate_buffsize(XSQLDA *sqlda)
{
	XSQLVAR *var;
	long cols;
	short dtp;
	long offset = 0;
	long alignment;
	long length;
	long count;

	cols = sqlda->sqld;
	var = sqlda->sqlvar;
	for (count = 0; count < cols; var++,count++) {
		length = alignment = var->sqllen;
		dtp = var->sqltype & ~1;

		if (dtp == SQL_TEXT) {
			alignment = 1;
		} else if (dtp == SQL_VARYING) {
			length += sizeof(short);
			alignment = sizeof(short);
		}

		offset = FB_ALIGN(offset, alignment);
		offset += length;
		offset = FB_ALIGN(offset, sizeof(short));
		offset += sizeof(short);
	}

	return offset + sizeof(short);
}

XSQLVAR* sqlda_sqlvar(XSQLDA* sqlda, ISC_SHORT col) {
	return sqlda->sqlvar + col;
}

char * fb_error_msg(const ISC_STATUS *isc_status)
{
	char msg[1024];
	int bufSize = sizeof(msg) + strlen("\n") + 1;
	char *result = malloc(bufSize);
	result[0] = '\0';
	while (fb_interpret(msg, 1024, &isc_status))
	{
		int msgSize = strlen(msg) + strlen("\n");
		if (bufSize < strlen(result) + msgSize)
		{
			bufSize += msgSize;
			result = realloc(result, bufSize);
		}
		strncat(result, msg, 1024);
		strcat(result, "\n");
	}
	return result;
}
