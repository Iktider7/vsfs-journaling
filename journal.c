#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>  

#define BLOCK_SIZE 4096
#define JOURNAL_MAGIC 0x4A524E4C
#define FS_MAGIC 0x56534653
#define REC_DATA 1
#define REC_COMMIT 2
#define NAME_LEN 28
#define JOURNAL_START_BLOCK 1

struct journal_header {
    uint32_t magic;
    uint32_t nbytes_used;
};

struct rec_header {
    uint16_t type;
    uint16_t size;
};

#pragma pack(push, 1)
struct data_record {
    struct rec_header hdr;
    uint32_t block_no;
    uint8_t data[BLOCK_SIZE];
};
#pragma pack(pop)

struct commit_record {
    struct rec_header hdr;
};

struct superblock {
    uint32_t magic;
    uint32_t block_size;
    uint32_t total_blocks;
    uint32_t inode_count;
    uint32_t journal_block;
    uint32_t inode_bitmap;
    uint32_t data_bitmap;
    uint32_t inode_start;
    uint32_t data_start;
    uint8_t _pad[128 - 9*4];
};

struct inode {
    uint16_t type;
    uint16_t links;
    uint32_t size;
    uint32_t direct[8];
    uint32_t ctime;
    uint32_t mtime;
    uint8_t _pad[128 - (2+2+4+8*4+4+4)];
};

struct dirent {
    uint32_t inode;
    char name[NAME_LEN];
};

int fd;

void read_block(int block_no, void* buffer);
void write_block(int block_no, void* buffer);
void read_journal_header(struct journal_header* jh);
void write_journal_header(struct journal_header* jh);
int journal_create(const char* name);
int journal_install();

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Usage: %s create <name> | install\n", argv[0]);
        return 1;
    }

    fd = open("vsfs.img", O_RDWR);
    if (fd < 0) {
        perror("Failed to open vsfs.img");
        return 1;
    }

    if (strcmp(argv[1], "create") == 0 && argc == 3) {
        return journal_create(argv[2]);
    } else if (strcmp(argv[1], "install") == 0) {
        return journal_install();
    } else {
        printf("Invalid command\n");
        close(fd);
        return 1;
    }
}

void read_block(int block_no, void* buffer) {
    lseek(fd, block_no * BLOCK_SIZE, SEEK_SET);
    ssize_t bytes_read = read(fd, buffer, BLOCK_SIZE);
    if (bytes_read != BLOCK_SIZE) {
        perror("read_block failed");
        exit(1);
    }
}

void write_block(int block_no, void* buffer) {
    lseek(fd, block_no * BLOCK_SIZE, SEEK_SET);
    ssize_t bytes_written = write(fd, buffer, BLOCK_SIZE);
    if (bytes_written != BLOCK_SIZE) {
        perror("write_block failed");
        exit(1);
    }
}

void read_journal_header(struct journal_header *jh) {
    uint8_t block[BLOCK_SIZE];
    read_block(JOURNAL_START_BLOCK, block);
    memcpy(jh, block, sizeof(struct journal_header));
}

void write_journal_header(struct journal_header *jh) {
    uint8_t block[BLOCK_SIZE];
    read_block(JOURNAL_START_BLOCK, block);
    memcpy(block, jh, sizeof(struct journal_header));
    write_block(JOURNAL_START_BLOCK, block);
}

int find_free_inode(uint8_t *inode_bitmap, uint32_t inode_count) {
    for (uint32_t i = 0; i < inode_count; i++) {
        uint32_t byte = i / 8;
        uint32_t bit = i % 8;
        if (!(inode_bitmap[byte] & (1 << bit))) {
            return i;
        }
    }
    return -1;
}

int find_free_dirent_slot(struct dirent *dir_block) {
    int max_entries = BLOCK_SIZE / sizeof(struct dirent);
    for (int i = 0; i < max_entries; i++) {
        if (dir_block[i].inode == 0 && dir_block[i].name[0] == '\0') {
            return i;
        }
    }
    return -1;
}

void append_record_to_journal(struct journal_header *jh, void *record, uint16_t record_size) {
    uint32_t journal_bytes = jh->nbytes_used;
    uint32_t bytes_remaining = record_size;
    uint32_t bytes_written = 0;
    uint8_t *record_ptr = (uint8_t *)record;
    
    while (bytes_remaining > 0) {
        uint32_t current_block = JOURNAL_START_BLOCK + (journal_bytes / BLOCK_SIZE);
        uint32_t current_offset = journal_bytes % BLOCK_SIZE;
        uint32_t space_in_block = BLOCK_SIZE - current_offset;
        uint32_t chunk_size = (bytes_remaining < space_in_block) ? bytes_remaining : space_in_block;
        
        uint8_t journal_block_data[BLOCK_SIZE];
        read_block(current_block, journal_block_data);
        memcpy(journal_block_data + current_offset, record_ptr + bytes_written, chunk_size);
        write_block(current_block, journal_block_data);
        
        bytes_written += chunk_size;
        bytes_remaining -= chunk_size;
        journal_bytes += chunk_size;
    }
    
    jh->nbytes_used = journal_bytes;
}

void read_from_journal(uint32_t journal_pos, void *buffer, uint32_t size) {
    uint32_t bytes_remaining = size;
    uint32_t bytes_read = 0;
    uint8_t *buffer_ptr = (uint8_t *)buffer;
    
    while (bytes_remaining > 0) {
        uint32_t current_block = JOURNAL_START_BLOCK + (journal_pos / BLOCK_SIZE);
        uint32_t offset_in_block = journal_pos % BLOCK_SIZE;
        uint32_t space_in_block = BLOCK_SIZE - offset_in_block;
        uint32_t chunk_size = (bytes_remaining < space_in_block) ? bytes_remaining : space_in_block;
        
        uint8_t journal_block_data[BLOCK_SIZE];
        read_block(current_block, journal_block_data);
        memcpy(buffer_ptr + bytes_read, journal_block_data + offset_in_block, chunk_size);
        
        bytes_read += chunk_size;
        bytes_remaining -= chunk_size;
        journal_pos += chunk_size;
    }
}

int journal_create(const char* name) {
    printf("Creating file: %s\n", name);
    
    uint8_t sb_block[BLOCK_SIZE];
    read_block(0, sb_block);
    struct superblock sb;
    memcpy(&sb, sb_block, sizeof(struct superblock));
    
    if (sb.magic != FS_MAGIC) {
        printf("ERROR: Invalid filesystem magic!\n");
        return -1;
    }
    
    struct journal_header jh;
    read_journal_header(&jh);
    
    if (jh.magic != JOURNAL_MAGIC) {
        jh.magic = JOURNAL_MAGIC;
        jh.nbytes_used = sizeof(struct journal_header);
        write_journal_header(&jh);
    }
    
    if (jh.nbytes_used > sizeof(struct journal_header)) {
        printf("ERROR: Journal has pending transactions.\n");
        printf("Please run 'journal install' before creating new files.\n");
        return -1;
    }
    
    uint8_t inode_bitmap[BLOCK_SIZE];
    read_block(sb.inode_bitmap, inode_bitmap);
    
    int free_inode = find_free_inode(inode_bitmap, sb.inode_count);
    if (free_inode == -1) {
        printf("ERROR: No free inodes available!\n");
        return -1;
    }
    
    uint8_t root_dir_block[BLOCK_SIZE];
    read_block(sb.data_start, root_dir_block);
    struct dirent *dir_entries = (struct dirent *)root_dir_block;
    
    int free_slot = find_free_dirent_slot(dir_entries);
    if (free_slot == -1) {
        printf("ERROR: No free directory slots!\n");
        return -1;
    }
    
    // Update inode bitmap
    uint8_t new_inode_bitmap[BLOCK_SIZE];
    memcpy(new_inode_bitmap, inode_bitmap, BLOCK_SIZE);
    uint32_t byte_idx = free_inode / 8;
    uint32_t bit_idx = free_inode % 8;
    new_inode_bitmap[byte_idx] |= (1 << bit_idx);
    
    // Update inode table - includes BOTH new file inode AND root inode
    uint32_t inodes_per_block = BLOCK_SIZE / sizeof(struct inode);
    uint32_t inode_block_index = free_inode / inodes_per_block;
    uint32_t inode_slot = free_inode % inodes_per_block;
    
    uint8_t inode_block[BLOCK_SIZE];
    read_block(sb.inode_start + inode_block_index, inode_block);
    
    uint8_t new_inode_block[BLOCK_SIZE];
    memcpy(new_inode_block, inode_block, BLOCK_SIZE);
    
    struct inode *inodes = (struct inode *)new_inode_block;
    
    // Create new file inode
    struct inode *new_inode = &inodes[inode_slot];
    new_inode->type = 1;
    new_inode->links = 1;
    new_inode->size = 0;
    memset(new_inode->direct, 0, sizeof(new_inode->direct));
    new_inode->ctime = (uint32_t)time(NULL);
    new_inode->mtime = (uint32_t)time(NULL);
    
    // CRITICAL: Update root inode size (root is always inode 0 in first block)
    if (inode_block_index == 0) {
        struct inode *root_inode = &inodes[0];
        root_inode->size += sizeof(struct dirent);
        root_inode->mtime = (uint32_t)time(NULL);
    }
    
    // Update root directory
    uint8_t new_root_dir[BLOCK_SIZE];
    memcpy(new_root_dir, root_dir_block, BLOCK_SIZE);
    struct dirent *new_dir_entries = (struct dirent *)new_root_dir;
    new_dir_entries[free_slot].inode = free_inode;
    strncpy(new_dir_entries[free_slot].name, name, NAME_LEN);
    new_dir_entries[free_slot].name[NAME_LEN - 1] = '\0';
    
    // Check journal capacity
    uint32_t data_record_size = sizeof(struct data_record);
    uint32_t commit_record_size = sizeof(struct commit_record);
    uint32_t total_transaction_size = (3 * data_record_size) + commit_record_size;
    uint32_t journal_capacity = 16 * BLOCK_SIZE;
    
    if (jh.nbytes_used + total_transaction_size > journal_capacity) {
        printf("ERROR: Journal full! Run 'journal install' first.\n");
        return -1;
    }
    
    // Append records to journal
    struct data_record bitmap_record;
    bitmap_record.hdr.type = REC_DATA;
    bitmap_record.hdr.size = data_record_size;
    bitmap_record.block_no = sb.inode_bitmap;
    memcpy(bitmap_record.data, new_inode_bitmap, BLOCK_SIZE);
    append_record_to_journal(&jh, &bitmap_record, data_record_size);
    
    struct data_record inode_record;
    inode_record.hdr.type = REC_DATA;
    inode_record.hdr.size = data_record_size;
    inode_record.block_no = sb.inode_start + inode_block_index;
    memcpy(inode_record.data, new_inode_block, BLOCK_SIZE);
    append_record_to_journal(&jh, &inode_record, data_record_size);
    
    struct data_record dir_record;
    dir_record.hdr.type = REC_DATA;
    dir_record.hdr.size = data_record_size;
    dir_record.block_no = sb.data_start;
    memcpy(dir_record.data, new_root_dir, BLOCK_SIZE);
    append_record_to_journal(&jh, &dir_record, data_record_size);
    
    struct commit_record commit_rec;
    commit_rec.hdr.type = REC_COMMIT;
    commit_rec.hdr.size = commit_record_size;
    append_record_to_journal(&jh, &commit_rec, commit_record_size);
    
    write_journal_header(&jh);
    
    printf("File '%s' creation journaled successfully.\n", name);
    return 0;
}

int journal_install() {
    struct journal_header jh;
    read_journal_header(&jh);
    
    if (jh.magic != JOURNAL_MAGIC) {
        printf("ERROR: Journal not initialized!\n");
        return -1;
    }
    
    if (jh.nbytes_used == sizeof(struct journal_header)) {
        printf("Journal is empty - nothing to install.\n");
        return 0;
    }

    uint32_t current_pos = sizeof(struct journal_header);
    uint8_t data_to_replay[16][BLOCK_SIZE];
    uint32_t block_numbers[16];
    int block_count = 0;
    int transactions_installed = 0;
    
    while (current_pos < jh.nbytes_used) {
        struct rec_header hdr;
        read_from_journal(current_pos, &hdr, sizeof(struct rec_header));
        
        if (hdr.type == REC_DATA) {
            struct data_record data_rec;
            read_from_journal(current_pos, &data_rec, sizeof(struct data_record));
            
            if (block_count >= 16) {
                printf("ERROR: Too many data records in one transaction!\n");
                return -1;
            }
            
            memcpy(data_to_replay[block_count], data_rec.data, BLOCK_SIZE);
            block_numbers[block_count] = data_rec.block_no;
            block_count++;
            
        } else if (hdr.type == REC_COMMIT) {
            for (int i = 0; i < block_count; i++) {
                write_block(block_numbers[i], data_to_replay[i]);
            }
            
            transactions_installed++;
            block_count = 0;
            
        } else {
            printf("ERROR: Unknown record type %u\n", hdr.type);
            return -1;
        }
        
        current_pos += hdr.size;
    }
    
    if (block_count > 0) {
        printf("WARNING: Found uncommitted data records - discarding.\n");
    }

    jh.nbytes_used = sizeof(struct journal_header);
    write_journal_header(&jh);
    
    printf("Journal installed successfully. %d transaction(s) applied.\n", transactions_installed);
    return 0;
}