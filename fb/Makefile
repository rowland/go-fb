include $(GOROOT)/src/Make.inc

TARG=github.com/rowland/go-fb/fb

CGOFILES=\
	database.go\
	connection.go\
	cursor.go\

CGO_OFILES=\
	fb.o\

CGO_CFLAGS = -D_XOPEN_SOURCE -D_DARWIN_C_SOURCE -I/Library/Frameworks/Firebird.framework/Headers
CGO_LDFLAGS = -L. -arch x86_64 -framework Firebird

include $(GOROOT)/src/Make.pkg
