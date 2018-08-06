
# 指令编译器和选项
CC = g++
DEF = __KERNEL__ _FILE_OFFSET_BITS=64
LIBS = fuse3
INCLUDEDIR = -Iinclude
INCLUDEDIR += $(shell pkg-config --cflags $(LIBS))
OPTFLAGS = -Wall  -Wextra -pthread
OBJ_DIR = obj
CXXFLAGS = -std=c++17 -pthread
CXXFLAGS += $(addprefix -D,$(DEF))

TARGET = rocksdb-fuse

LDFLAGS = $(shell pkg-config $(LIBS) --libs)
LDFLAGS += -lstdc++fs -lrocksdb
LDFLAGS += $(OPTFLAGS)

CC = g++
SHELL = bash
SOURCES = $(shell find -name "*.cpp" | sed 's/^\.\///g')
OBJS = $(addprefix $(OBJ_DIR)/,$(SOURCES:.cpp=.o))

.PHONY: all clean

sinclude $(DEPFILES)

all: $(TARGET)
$(OBJ_DIR)/%.o: %.cpp
	@mkdir -p $(dir $@)
	@echo "[cc] $< ..."
	@$(CC) -c $< -o $@ $(CXXFLAGS) $(INCLUDEDIR)

$(TARGET): $(OBJS)
	@echo "Linking ..."
	@$(CC) $(OBJS) -o $@ $(LDFLAGS)
	@echo "done."

$(OBJ_DIR)/%.dep: %.cpp Makefile
	@mkdir -p $(dir $@)
	@echo "[dep] $< ..."
	@$(CC) $(CXXFLAGS) -MM -MT "$(OBJ_DIR)/$(<:.cpp=.o) $(OBJ_DIR)/$(<:.cpp=.dep)" "$<"  > "$@"


clean:
	rm -rf ./$(TARGET) ./$(OBJ_DIR)