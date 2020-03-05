#include "./sfs.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

uint32_t varcharSize(const SFSVarchar* varchar)
{
    return sizeof(uint32_t) + varchar->len;
}

SFSVarchar* readVarchar(SFSTable** ptable, FILE* fp)
{
    uint32_t size;
    fread(&size, 4, 1, fp);
    char* s = (char*)malloc(size);
    fread(s, 1, size, fp);
    SFSVarchar* varchar = sfsTableAddVarchar(ptable, size, s);
    return varchar;
}

int sfsVarcharCons(SFSVarchar* varchar, const char* src)
{
    char* s = (char*)&varchar + 4;
    for (uint32_t i = 0; src[i] != '\0'; i++)
        s[i] = src[i];
    return 0;
}

uint32_t recordSize(const SFSVarchar* varchar)
{
    uint32_t size = *((uint32_t*)varchar);
    uint32_t reval = 0;
    for (uint16_t i = 0; i < size; i++)
        reval += ((char*)varchar)[i + sizeof(uint32_t)];
    return reval;
}

SFSVarchar* sfsVarcharCreate(uint32_t varcharSize, const char* src)
{
    //char* s = (char*)malloc(sizeof(uint32_t) + varcharSize);
    SFSVarchar* varchar = (SFSVarchar*)malloc(sizeof(uint32_t) + varcharSize);
    memcpy(&varchar->len, &varcharSize, sizeof(uint32_t));
    memcpy(&varchar->buf, (void*)src, varcharSize);
    return varchar;
}
int sfsVarcharRelease(SFSVarchar* varchar)
{
    free(varchar);
    return 0;
}

int sfsTableCons(SFSTable* table, uint32_t initStorSize, const SFSVarchar* recordMeta, SFSDatabase* db)
{
    table->size = initStorSize + sizeof(uint32_t) * 9 + varcharSize(recordMeta);
    table->storSize = 0;
    table->freeSpace = initStorSize;
    table->varcharNum = 0;
    table->recordNum = 0;
    table->recordSize = recordSize(recordMeta);

    table->recordMeta = (SFSVarchar*)recordMeta;
    table->lastVarchar = (SFSVarchar*)(&table->buf[initStorSize]);
    table->database = db;
    return 0;
}
SFSTable* sfsTableCreate(uint32_t initStorSize, const SFSVarchar* recordMeta, SFSDatabase* db)
{
    uint32_t table_size = initStorSize + sizeof(uint32_t) * 9 + varcharSize(recordMeta);

    //char* s = (char*)malloc(table_size);
    SFSTable* table = (SFSTable*)malloc(sizeof(SFSTable) + initStorSize);

    table->size = table_size;
    table->storSize = 0;
    table->freeSpace = initStorSize;
    table->varcharNum = 0;
    table->recordNum = 0;
    table->recordSize = recordSize(recordMeta);

    table->recordMeta = (SFSVarchar*)recordMeta;
    table->lastVarchar = (SFSVarchar*)(&(table->buf[initStorSize]));
    table->database = db;

    db->table[db->tableNum++] = table;
    return table;
}
int sfsTableRelease(SFSTable* table)
{
    for (uint32_t i = 0; i < table->recordNum; i += sizeof(int*)) {
        free((void*)table->buf[i * sizeof(int*)]);
    }
    for (uint32_t i = 0; i < table->varcharNum; i += sizeof(int*)) {
        sfsVarcharRelease((SFSVarchar*)(table->lastVarchar - i*sizeof(void*)));
    }
    return 0;
}
int sfsTableReserve(SFSTable** table, uint32_t storSize)
{
    SFSTable* newtable = sfsTableCreate(storSize, (*table)->recordMeta, (*table)->database);
    memcpy(newtable->buf, (*table)->buf, (*table)->recordSize);
    memcpy((void*)newtable->buf[newtable->size - (*table)->varcharNum * sizeof(int*)], (*table)->lastVarchar, sizeof(int*) * (*table)->varcharNum);
    sfsTableRelease(*table);
    table = &newtable;
    return 0;
}

void* sfsTableAddRecord(SFSTable** ptable)
{
    if ((*ptable)->freeSpace <= (*ptable)->recordSize)
        sfsTableReserve(ptable, (*ptable)->storSize * 2);
    (*ptable)->recordNum++;
    (*ptable)->freeSpace -= (*ptable)->recordSize;
    (*ptable)->storSize += (*ptable)->recordSize;
    return &((*ptable)->buf[((*ptable)->recordNum - 1) * (*ptable)->recordSize]);
}

SFSVarchar* sfsTableAddVarchar(SFSTable** ptable, uint32_t varcharLen, const char* src)
{
    if ((*ptable)->freeSpace <= (*ptable)->recordSize)
        sfsTableReserve(ptable, (*ptable)->storSize * 2);

    SFSVarchar* varchar = sfsVarcharCreate(varcharLen, src);
    (*ptable)->size += varcharSize(varchar);
    (*ptable)->freeSpace -= sizeof(void*);
    (*ptable)->storSize += sizeof(void*);

    memcpy((*ptable)->lastVarchar - (++(*ptable)->varcharNum) * sizeof(void*), varchar, sizeof(void*));
    (*ptable)->lastVarchar -= sizeof(void*);
    return varchar;
}

SFSDatabase* sfsDatabaseCreate()
{
	SFSDatabase* db = (SFSDatabase*)malloc(sizeof(SFSDatabase));
	db->magic = 0x534653aa;
	db->crc = 0;
    db->version = 123;
    db->size = sizeof(SFSDatabase);
    db->tableNum = 0;
    //db->pad;
    //db->table;
    //char buf[]
    return db;
}
void sfsDatabaseRelease(SFSDatabase* db)
{
    for (uint32_t i = 0; i < db->tableNum; i++)
        sfsTableRelease(db->table[i]);
    free(db);
}


int sfsTableSave(FILE* fp, SFSTable* table,uint32_t offset)
{
    uint32_t tablesize = 0;
    char* s = (char*)malloc(10000);

    memcpy(s, &table->size, sizeof(uint32_t) * 6);
    *(int*)(s + 24) = offset + table->size - varcharSize(table->recordMeta);
    *(int*)(s + 28) = offset + table->size - varcharSize(table->recordMeta) - table->varcharNum * sizeof(void*);
    *(int*)(s + 32) = offset;

    uint32_t i, j;
    for (i = 0; i < table->recordNum; i++)
        memcpy(s + 36 + i * table->recordSize, &table->buf[i * table->recordSize], table->recordSize);

    for (j = 0; j < table->varcharNum; j++) {
        SFSVarchar* v = (SFSVarchar*)(table->lastVarchar + sizeof(void*) * j);
        uint32_t varcharsize = varcharSize(v);
        memcpy(s + 36 + table->recordNum * table->recordSize + tablesize, &((v)->len), varcharsize);
        tablesize += varcharsize;
    }

    memcpy(s + 36 + table->recordNum * table->recordSize + tablesize, &(table->recordMeta)->len, varcharSize(table->recordMeta));
    *(int*)s = 36 + table->recordNum * table->recordSize + tablesize;
    fwrite(s, 36 + table->recordNum * table->recordSize + tablesize + varcharSize(table->recordMeta), 1, fp);

    free(s);

    return tablesize;
}

int sfsDatabaseSave(char* fileName, SFSDatabase* db)
{
    FILE* fp;
    if (NULL == (fp = fopen(fileName, "wb")))
        sfsErrMsg();
    fwrite(&db->magic, sizeof(SFSDatabase), 1, fp);
    for (uint32_t i = 0; i < db->tableNum; i++) {
        fseek(fp, 0, SEEK_END);
        sfsTableSave(fp, db->table[i], ftell(fp));
    }
    fclose(fp);
    return 0;
}

SFSDatabase* sfsDatabaseCreateLoad(const char* fileName)
{
    FILE* fp;
    if (NULL == (fp = fopen(fileName, "rb")))
        sfsErrMsg();

    SFSDatabase* db = sfsDatabaseCreate();
    fread(&db->magic, sizeof(SFSDatabase), 1, fp);
    if (db->magic != 0x534653aa)
        sfsErrMsg();
    for (uint32_t i = 0; i < db->tableNum; i++) {
        uint32_t varcharLen;
        char* s = (char*)malloc(1000);
        uint32_t* tableHead = (uint32_t*)malloc(sizeof(uint32_t) * 9);

        fread(&tableHead, sizeof(uint32_t), 9, fp);
        SFSTable* table = sfsTableCreate(tableHead[1] + tableHead[2], NULL, db);

        memcpy(&table->size, tableHead, sizeof(uint32_t) * 6);
        table->database = db;
        for (uint32_t j = 0; j < tableHead[4]; j++)//����records
        {
            fread(&table->buf[j * table->recordSize], table->recordSize, 1, fp);//��record��
            for (uint32_t k = 0; k < table->varcharNum; k++) {
                fread(&varcharLen, 4, 1, fp);
                fread(s, 1, varcharLen, fp);
                sfsTableAddVarchar(&table, varcharLen, s);
            }
            fread(&varcharLen, 4, 1, fp);
            fread(s, 1, varcharLen, fp);
            table->recordMeta = sfsVarcharCreate(varcharLen, s);
        }
        free(s);
        free(tableHead);
    }
    fclose(fp);
    return db;
}

SFSTable* sfsDatabaseAddTable(SFSDatabase* db, uint32_t storSize, const SFSVarchar* recordMeta)
{
    SFSTable* table = sfsTableCreate(storSize, recordMeta, db);
    db->table[db->tableNum++] = table;
    return table;
}


// return the lastest err
char* sfsErrMsg()
{
    printf("There is an error\n");
    exit(0);
    return NULL;
}