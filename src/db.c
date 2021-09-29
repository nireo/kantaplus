#include "db.h"
#include <stdlib.h>
#include <string.h>

static void serialize_row(Row *src, void *dst) {
  memcpy(dst + ID_OFFSET, &(src->id), ID_SIZE);
  memcpy(dst + INPUT1_OFFSET, &(src->input1), INPUT1_SIZE);
  memcpy(dst + INPUT2_OFFSET, &(src->input2), INPUT2_SIZE);
}

static void deserialize_row(void *src, Row *dst) {
  memcpy(&(dst->id), src + ID_OFFSET, ID_SIZE);
  memcpy(&(dst->input1), src + INPUT1_OFFSET, INPUT1_SIZE);
  memcpy(&(dst->input2), src + INPUT2_OFFSET, INPUT2_SIZE);
}

static void *row_slot(Table *table, uint32_t row_count) {
  uint32_t page_number = row_count / ROWS_PER_PAGE;
  void *page = table->pages[page_number];
  if (page == NULL) {
    page = table->pages[page_number] = malloc(PAGE_SIZE);
  }

  uint32_t row_offset = row_count % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;

  return page + byte_offset;
}
