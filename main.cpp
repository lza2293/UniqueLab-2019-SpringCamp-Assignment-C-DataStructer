#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "./sfs.h"
#include "./greatest/greatest.h"

typedef struct A{
    int8_t x0_1;
    SFSVarchar *x2_v;
    char x3_10[10];
}A;
char AMeta_c[] = {3, 0, 0, 0, 1, 0, 10};
SFSVarchar *AMeta = (SFSVarchar *)AMeta_c;
/*  .len = 5,
    .buf = {1,4,0,10,0} */
const uint32_t ArecordSize = sizeof(A);

int main(){
	SFSDatabase *db = sfsDatabaseCreate();
	SFSTable *table = sfsTableCreate(5 * ArecordSize, AMeta, db);
    A* record = (A*)sfsTableAddRecord(&table);
    record->x0_1 = 2;
    record->x2_v = sfsTableAddVarchar(&table, 4, "test");
    sfsDatabaseSave((char*)"file.sfs", db);
    return 0;
}
