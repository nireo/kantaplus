#include "db.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

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

static void *get_page(Pager *pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("page num out of bounds.\n");
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    void *page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->f_len / PAGE_SIZE;

    if (pager->f_len % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      lseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_desc, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("cannot read file\n");
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;
  }

  return pager->pages[page_num];
}

static void *cursor_slot(Cursor *cursor) {
  uint32_t row_count = cursor->row_num;
  uint32_t page_number = row_count / ROWS_PER_PAGE;
  void *page = get_page(cursor->table->pager, page_number);

  uint32_t row_offset = row_count % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;

  return page + byte_offset;
}

Pager *pager_open(const char *fname) {
  int fd = open(fname, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd == -1) {
    printf("unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t f_len = lseek(fd, 0, SEEK_END);

  Pager *pg = malloc(sizeof(Pager));
  pg->file_desc = fd;
  pg->f_len = f_len;

  for (uint32_t i = 0; i < TABLE_MAX_ROWS; ++i) {
    pg->pages[i] = NULL;
  }

  return pg;
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size) {
  if (pager->pages[page_num] == NULL) {
    printf("cannot flush null");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_desc, page_num * PAGE_SIZE, SEEK_SET);
  if (offset == -1) {
    printf("error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_desc, pager->pages[page_num], size);
  if (bytes_written == -1) {
    printf("error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

Table *db_open(const char *fname) {
  Pager *pager = pager_open(fname);
  uint32_t row_count = pager->f_len / ROW_SIZE;

  Table *table = malloc(sizeof(Table));
  table->pager = pager;
  table->row_count = row_count;

  return table;
}

void db_close(Table *table) {
  Pager *pager = table->pager;
  uint32_t f_pages = table->row_count / ROWS_PER_PAGE;

  for (uint32_t i = 0; i < f_pages; ++i) {
    if (pager->pages[i] == NULL)
      continue;

    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  uint32_t add_rows = table->row_count % ROWS_PER_PAGE;
  if (add_rows > 0) {
    uint32_t page_num = add_rows;
    if (pager->pages[page_num] != NULL) {
      pager_flush(pager, page_num, add_rows * ROW_SIZE);
      free(pager->pages[page_num]);
      pager->pages[page_num] = NULL;
    }
  }

  int result = close(pager->file_desc);
  if (result == -1) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

  free(pager);
}

Cursor *table_start(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = 0;
  cursor->end_of_table = (table->row_count == 0);

  return cursor;
}

Cursor *table_end(Table *table) {
  Cursor *cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = table->row_count;
  cursor->end_of_table = true;

  return cursor;
}

void *cursor_value(Cursor *cursor) {
  uint32_t row_num = cursor->row_num;
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void *page = get_page(cursor->table->pager, page_num);
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;

  return page + byte_offset;
}

void cursor_advance(Cursor *cursor) {
  cursor->row_num += 1;
  if (cursor->row_num >= cursor->table->row_count) {
    cursor->end_of_table = true;
  }
}

uint32_t *leaf_node_num_cells(void *node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void *leaf_node_cell(void *node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t *leaf_node_key(void *node, uint32_t cell_num) {
  return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void *node) { *leaf_node_num_cells(node) = 0; }
