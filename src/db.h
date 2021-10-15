#ifndef DB_H
#define DB_H

#include <stdbool.h>
#include <stdint.h>

#define INPUT_1_SIZE 32
#define INPUT_2_SIZE 255

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;

#define size_attr(strct, attr) sizeof(((strct *)0)->attr)

typedef struct {
  uint32_t id;
  char input1[INPUT_1_SIZE + 1];
  char input2[INPUT_2_SIZE + 1];
} Row;

const uint32_t ID_SIZE = size_attr(Row, id);
const uint32_t INPUT1_SIZE = size_attr(Row, input1);
const uint32_t INPUT2_SIZE = size_attr(Row, input2);

const uint32_t ID_OFFSET = 0;
const uint32_t INPUT1_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t INPUT2_OFFSET = INPUT1_OFFSET + INPUT1_SIZE;

const uint32_t ROW_SIZE = ID_SIZE + INPUT1_SIZE + INPUT2_SIZE;

const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
  int file_desc;
  uint32_t f_len;
  void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
  uint32_t row_count;
  Pager *pager;
} Table;

typedef struct {
  Table *table;
  uint32_t row_num;
  bool end_of_table;
} Cursor;

Table *db_open(const char *fname);

typedef enum { NODE_INTERNAL, NODE_LEAF } NodeType;

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE =
    NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

#endif
