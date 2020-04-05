#include <stdlib.h>
#include "buffer_mgr.h"
#include <stdio.h>
#include "record_mgr.h"
#include "storage_mgr.h"
#include <string.h>


// Defination of the Structure 
typedef struct RecordManager
{
    BM_BufferPool bufpool;
    // variable referencing buffer pool
    int count; // to maintain the count of pages
    int tupcnt;
    // to maintain count of tuple
    BM_PageHandle pghdlr; // refers to page handler
    RID recoID; // refers to the record ID
    int pgFree;
    // refers to the count of free pages
    Expr *expcond;
    // refers to status
  
} RecMgr;
// alias of the structure

// initializing global variables
const int Max_Pages = 100;
// initializing max pages count
const int pgSIZE = 15; // initializing page size
RecMgr *recMgr;
// pointer of type record manager

// function for checking available slots
int slotavail(char *info, int RecSize)
{
    int noslot; // integer that refers to the slots available
    noslot = PAGE_SIZE / RecSize;
    // calculating number of slots
    int i;
    // iterating through the records 
    for (i = 0; i < noslot; i++)
        if (info[i * RecSize] != '+') // checking the condition for '+'
        {
            return i;
            // returns the index
        }
    return -1; // else returns -1
}

// initializing record manager
extern RC initRecordManager (void *mgmtData)
{
    initStorageManager();
    // calls record manager
    return RC_OK;
    // returns success
}

// function used to shutdown record manager
extern RC shutdownRecordManager ()
{
    recMgr = NULL; // assigning to NULL
    free(recMgr);
    // deallocates the pointer
    return RC_OK;
    // returns success
}

// funciton to create table
extern RC createTable (char *name, Schema *pgschema)
{
    recMgr = (RecMgr*) malloc(sizeof(RecMgr));
    // allocating mamery using malloc
    initBufferPool(&recMgr->bufpool, name, Max_Pages, RS_LRU, NULL);
    // calls initBufferPool with appropriate arguments
    char data[PAGE_SIZE]; // refers to data size of Page record
    char *pgHandle = data; 
    // assigning page handler to the data block
    *(int*)pgHandle = 0;
    // assigning the value of the block to 0 initially
    pgHandle = pgHandle + sizeof(int); // incrementing the page handler accordingly
    *(int*)pgHandle = 1;
    // assigning the value to 1
    pgHandle = pgHandle + sizeof(int);
    // incrementing the position of the page handler
    *(int*)pgHandle = pgschema->numAttr; // assigning the structure member to the page handler
    pgHandle = pgHandle + sizeof(int);
    // incrementing the position of the page handler
    *(int*)pgHandle = pgschema->keySize;
    // assigning the structure member to the page handler
    pgHandle = pgHandle + sizeof(int);
    // incrementing the position of the page handler
    int i;
    // itreating through the page schema
    for(i = 0; i < pgschema->numAttr; i++)
        {
            strncpy(pgHandle, pgschema->attrNames[i], pgSIZE);
            // does copy of page schema
            pgHandle = pgHandle + pgSIZE; // incrementing page handler
            *(int*)pgHandle = (int)pgschema->dataTypes[i];
            // manipulating page handler data
            pgHandle = pgHandle + sizeof(int);
            // incrementing page handler accordingly
            *(int*)pgHandle = (int) pgschema->typeLength[i]; // manipulating data of the page handler 
            pgHandle = pgHandle + sizeof(int);
            // incrementing page handler with respect to size of int
        }
    //initialixzing file handler
    SM_FileHandle fileHandle;
    int res;
    if((res = createPageFile(name)) != RC_OK)
    //creating page file and checking for success condition
        return res; //returns status
    if((res = openPageFile(name, &fileHandle)) != RC_OK)
    //opening page file and checking for success condition
        return res;//returns status
    if((res = writeBlock(0, &fileHandle, data)) != RC_OK)
    //writing page file and checking for success condition
        return res;//returns status
    if((res = closePageFile(&fileHandle)) != RC_OK)
    //closing page file and checking for success condition
        return res;//returns status
    return RC_OK;
    //returns success
}


//function to open the table
extern RC openTable (RM_TableData *rel, char *name)
{
    int cnt;//variable to maintain count
    SM_PageHandle pgHandle;
    //intiializing page handler
    rel->name = name; //assigning name to structure member
    Schema *pgschema;
    //intiializing page schema
    rel->mgmtData = recMgr; //assigning data to structure member
    pinPage(&recMgr->bufpool, &recMgr->pghdlr, 0);
    //calls the pin page function with appropriate arguments
    pgHandle = (char*) recMgr->pghdlr.data;
    //reassigning page handler
    recMgr->tupcnt= *(int*)pgHandle; //assigning tuple count to page handler data
    pgHandle = pgHandle + sizeof(int);
    //incrementing page handler
    recMgr->pgFree= *(int*) pgHandle;
    ///assigning record mgr data to page handler data
    pgHandle = pgHandle + sizeof(int);
    //incrementing page handler
    cnt = *(int*)pgHandle; //maintaining the count
    pgHandle = pgHandle + sizeof(int);
    //incrementing page handler
    pgschema = (Schema*) malloc(sizeof(Schema));
    //allocating memory using malloc
    pgschema->attrNames = (char**) malloc(sizeof(char*) *cnt);
    //allocating memory for attribute name
    pgschema->numAttr = cnt; //assigning count
    pgschema->typeLength = (int*) malloc(sizeof(int) *cnt);
    //allocating memory for attribute typelength
    pgschema->dataTypes = (DataType*) malloc(sizeof(DataType) *cnt);
    //allocating memory for data types of page schema
    int j;
    //iterating through page schema till count
    for(j = 0; j < cnt; j++)
    {
        pgschema->attrNames[j]= (char*) malloc(pgSIZE);
        //allocating page size memory to attribute name member
    }
    //iterating through page schema till value is assigned for schema
    for(j = 0; j < pgschema->numAttr; j++)
        {
        strncpy(pgschema->attrNames[j], pgHandle, pgSIZE);
        //copies the details of page schema
        pgHandle = pgHandle + pgSIZE;//incremnting page handler
        pgschema->dataTypes[j]= *(int*) pgHandle;
        //manipulating page schema 
        pgHandle = pgHandle + sizeof(int);//incremnting page handler
        pgschema->typeLength[j]= *(int*)pgHandle;
        //manipulating page schema 
        pgHandle = pgHandle + sizeof(int);//incremnting page handler
    }
    rel->schema = pgschema;
    //assigning schema to page schema
    unpinPage(&recMgr->bufpool, &recMgr->pghdlr);
    //calls unpin page
    forcePage(&recMgr->bufpool, &recMgr->pghdlr);
    //calls force page function
    return RC_OK; //returns success
}

//function to delete a table
extern RC deleteTable (char *name)
{
    destroyPageFile(name);
    //calls destroy page function
    return RC_OK;
    //returns success
}

//function to get the number of tuples
extern int getNumTuples (RM_TableData *rel)
{
    RecMgr *recMgr = rel->mgmtData; 
    //assigning record manager data
    return recMgr->tupcnt;
    //returns the tuple count 
}

//close the table
extern RC closeTable (RM_TableData *rel)
{
    RecMgr *recMgr = rel->mgmtData;
    //assigning record manager data
    shutdownBufferPool(&recMgr->bufpool);
    //calls shut down buffer pool function 
    return RC_OK;//returns success
}

//function to insert a record
extern RC insertRecord (RM_TableData *rel, Record *record)
{
    RecMgr *recMgr = rel->mgmtData;
    //assigning record manager data
    RID *rcdID = &record->id; //assigns to record id
    int RecSize = getRecordSize(rel->schema);
    //gets record size of each record
    rcdID->page = recMgr->pgFree;
    //refers to free slots 
    pinPage(&recMgr->bufpool, &recMgr->pghdlr, rcdID->page);
    //calls the pin page function 
    char *data; //refers to data
    data = recMgr->pghdlr.data;
    //assigining page handler data to data
    rcdID->slot = slotavail(data, RecSize); //refers to empty slots
    //while conditon to check for available slots
    while(rcdID->slot == -1)
    {
        unpinPage(&recMgr->bufpool, &recMgr->pghdlr);
        //unpins page
        rcdID->page++; //increments record id of page
        pinPage(&recMgr->bufpool, &recMgr->pghdlr, rcdID->page);
        //pins page
        data = recMgr->pghdlr.data;//assigining page handler data to data
        rcdID->slot = slotavail(data, RecSize);
        //assigns record slot to available slot
    }
    char *ptrSlot; //pointer to refer to page slot 
    ptrSlot = data; //refers to data block
    markDirty(&recMgr->bufpool, &recMgr->pghdlr);
    //dirty bit is marked for data which is changed
    ptrSlot = ptrSlot + (rcdID->slot * RecSize);
    //the pointer is incremented accordingly
    *ptrSlot = '+'; //is assigned to '+' symbol
    memcpy(++ptrSlot, record->data + 1, RecSize - 1);
    //does memory copy of the pointer data
    unpinPage(&recMgr->bufpool, &recMgr->pghdlr);
    //unpins page
    recMgr->tupcnt++; //increments tuple count
    pinPage(&recMgr->bufpool, &recMgr->pghdlr, 0);
    //pins page
    //return that it is ok
    return RC_OK;
}

//function to get record
extern RC getRecord (RM_TableData *rel, RID rcdID, Record *record)
{
    RecMgr *recMgr = rel->mgmtData;
    //assigning record manager data
    pinPage(&recMgr->bufpool, &recMgr->pghdlr, rcdID.page);//pins page record
    int noRec = getRecordSize(rel->schema);
    //gets the record size for each record
    char *ptrData = recMgr->pghdlr.data;
    //assigning record manager data to ptr data
    ptrData = ptrData + (rcdID.slot * noRec);
    //incrementing ptrdata accordingly
    if(*ptrData != '+') //checking for '+' 
        return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
        //returns error 
    else
    {
        record->id = rcdID; //assiging record id
        char *info = record->data;
        //assigning record data
        memcpy(++info, ptrData + 1, noRec - 1);
        //copying pointer info
    }
    unpinPage(&recMgr->bufpool, &recMgr->pghdlr); //unpins page
    return RC_OK;
    //returns success
}

//function to delete a record
extern RC deleteRecord (RM_TableData *rel, RID rcdID)
{
    //record manager structure
    RecMgr *recMgr = rel->mgmtData;
    //assigning record manager data
    pinPage(&recMgr->bufpool, &recMgr->pghdlr, rcdID.page);//pins page
    recMgr->pgFree = rcdID.page;
    //assigning to free page
    char *info = recMgr->pghdlr.data;
    //assiging page handler data
    int noRec = getRecordSize(rel->schema);
    //gets the record size
    //update the data
    info = info + (rcdID.slot * noRec); 
    //assiging '-' value to data
    *info = '-';
    markDirty(&recMgr->bufpool, &recMgr->pghdlr);
    //marking dirty bit as data is modified
    unpinPage(&recMgr->bufpool, &recMgr->pghdlr);//unpins page
    return RC_OK;
    //returns success
}

//update record function
extern RC updateRecord (RM_TableData *rel, Record *record)
{
    RecMgr *recMgr = rel->mgmtData;
    //assigning record manager data
    pinPage(&recMgr->bufpool, &recMgr->pghdlr, record->id.page);
    //pins page 
    int noRec = getRecordSize(rel->schema);
    //gets the no of records
    RID rcdID = record->id;//assiging record id
    char *info;
    //refers to data block
    info = recMgr->pghdlr.data;
    //refers to page handler data
    info = info + (rcdID.slot * noRec);//updating the data in record
    *info = '+';
    //assiging value as '+'
    memcpy(++info, record->data + 1, noRec - 1 );
    //copying pointer data
    markDirty(&recMgr->bufpool, &recMgr->pghdlr);
    //dirty bit is marked as data is modified
    unpinPage(&recMgr->bufpool, &recMgr->pghdlr);//unpins page
    return RC_OK;
    //returns success
}

//function to start the scan
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    if (cond == NULL) //null conditon is checked
    {
        return RC_SCAN_CONDITION_NOT_FOUND;
        //returns error 
    }
    openTable(rel, "ScanTable");
    //opens the table 
    RecMgr *scMg; //refers to scan manager
    RecMgr *tabMg; //refers to table manager
    scMg = (RecMgr*) malloc(sizeof(RecMgr));
    //allocating memory
    scan->mgmtData = scMg;
    //assigining data
    scMg->recoID.page = 1;
    //assigining record id of page
    scMg->recoID.slot = 0;
    //assigining slots of page
    scMg->count = 0;
    //assigining count 
    scMg->expcond = cond;
    //assigining conditon 
    tabMg = rel->mgmtData;//assigining data for table manager
    tabMg->tupcnt = pgSIZE;
    //assigining tuple count for table manager
    scan->rel= rel; //assigining scan manager data
    return RC_OK;
    //returns success
}

//function for the next
extern RC next (RM_ScanHandle *scan, Record *record)
{
    RecMgr *scMg = scan->mgmtData;//refers to scan manager data
    RecMgr *tabMg = scan->rel->mgmtData;//refers to table manager data
    Schema *pgschema = scan->rel->schema;//refers to page schema data
    if (scMg->expcond == NULL)//checks for null condition
    {
        return RC_SCAN_CONDITION_NOT_FOUND;
        //returns error condition
    }
    Value *res = (Value *) malloc(sizeof(Value));
    //allocating memory to pointer
    int noRec;
    noRec = getRecordSize(pgschema);
    //gets the record size of each record
    int noSlots;
    //refers to no of slots
    noSlots= PAGE_SIZE / noRec;
    //calculating slots
    int cnt;//refers to count of page
    cnt = scMg->count;
    //assiging scan manager count to count
    int tuplesCnt;
    tuplesCnt = tabMg->tupcnt;
    //assiging table manager count to tuples count
    if (tuplesCnt == 0)//checks if count is 0
    {
        return RC_RM_NO_MORE_TUPLES;
        //returns error
    }
    //looping through tuple count for scan manager
    while(cnt <= tuplesCnt)
    {
        if (cnt <= 0)//if count is <=0
        {
            scMg->recoID.page = 1;
            //updating record page 
            scMg->recoID.slot = 0;
            //updating record slot 
        }
        else
        {
            scMg->recoID.slot++;
            //incrementing record slot 
            if(scMg->recoID.slot >= noSlots)
            {
                //checks if no of slots available is less than the required slot
                scMg->recoID.slot = 0;
                //updating slot 
                scMg->recoID.page++;
                //incrementing record page
            }
        }
        pinPage(&tabMg->bufpool, &scMg->pghdlr, scMg->recoID.page);
        //pins page approptiately
        char *info;//refers to data pointer
        info = scMg->pghdlr.data;
        //assigning info to page handler info
        info = info + (scMg->recoID.slot * noRec);
        //updating info with respect to number of records
        record->id.page = scMg->recoID.page;
        //record id page is mapped to scan manager id
        record->id.slot = scMg->recoID.slot;
        //record id slot is mapped to scan manager slot
        char *infoPtr = record->data;
        //info pointer points to record data
        *infoPtr = '-'; //manipulating data
        memcpy(++infoPtr, info + 1, noRec - 1);
        //memory copy of pointer data 
        scMg->count++; //incrementing count of scan manager
        cnt++;//incrementing count
        evalExpr(record, pgschema, scMg->expcond, &res);
        //calls evalexpr function with page schema 
        if(res->v.boolV == TRUE) //checks for bool value
        {
            unpinPage(&tabMg->bufpool, &scMg->pghdlr);//unpins page
            return RC_OK;
            //returns success
        }
    }
    unpinPage(&tabMg->bufpool, &scMg->pghdlr);
    //unpins page
    scMg->recoID.page = 1;
    //updates scan manager page
    scMg->recoID.slot = 0;
    //updates scan manager slot
    scMg->count = 0;//updates scan manager's count
    return RC_RM_NO_MORE_TUPLES;
    //returns error
}

//function to close the scan
extern RC closeScan (RM_ScanHandle *scan)
{
    RecMgr *scMg = scan->mgmtData;
    //assiging data to scan manager
    RecMgr *recMgr = scan->rel->mgmtData;
    //assiging data to management data
    if(scMg->count > 0) //checking for condition
    {
        unpinPage(&recMgr->bufpool, &scMg->pghdlr);//unpins page
        scMg->count = 0;
        //updating count
        scMg->recoID.page = 1;
        //updating record id
        scMg->recoID.slot = 0;
        //updating slots
    }
        scan->mgmtData = NULL; //data is set to null
        free(scan->mgmtData);
        //deallocates memory
    return RC_OK;
    //returns success
}

//to get the size of the record
extern int getRecordSize (Schema *schema)
{
    int j;
    int noREC = 0; //refers to number of records
    //iterating through page schema
    for(j = 0; j < schema->numAttr; j++)
    {
        switch(schema->dataTypes[j]) //checking for data types in schema
        {
            case DT_INT: //checking for int
                noREC = noREC + sizeof(int);
                //updating record accordingly
                break;
            case DT_STRING://checking for string
                noREC = noREC + schema->typeLength[j];
                //updating record accordingly
                break;
            case DT_BOOL://checking for bool
                noREC = noREC + sizeof(bool);
                //updating record accordingly
                break;
            case DT_FLOAT://checking for float
                noREC = noREC + sizeof(float);
                //updating record accordingly
                break;
        }
    }
    return ++noREC;
    //returns incremented record
}

//function to create a schema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *pgSc = (Schema *) malloc(sizeof(Schema));
    // allocating memory ot page schema
    pgSc->attrNames = attrNames;
    //  updates attribute name of page schema
    pgSc->numAttr = numAttr;
    //  updates number attribute of page schema
    pgSc->typeLength = typeLength;
    //  updates type length of page schema
    pgSc->dataTypes = dataTypes; //  updates data type of page schema
    pgSc->keyAttrs = keys;
    //  updates key attributes of page schema
    pgSc->keySize = keySize;
    // updates key size of page schema
    return pgSc;
    //  returns page schema
}

//function to free the schema
extern RC freeSchema (Schema *schema)
{
    free(schema);
    //dealloactes the schema
    return RC_OK;
    //returns success
}

//function to create the record
extern RC createRecord (Record **record, Schema *schema)
{
    int noRec;
    //refers to number of records
    Record *newRec = (Record*) malloc(sizeof(Record));
    //allocating memory to new record
    noRec = getRecordSize(schema); // gets the record size
    newRec->data= (char*) malloc(noRec);
    // allocating memory to new record data
    newRec->id.page = newRec->id.slot = -1;
    // new record id is updated
    char *infoPtr = newRec->data;
    // pointer data is assigned to new record data
    *infoPtr = '-'; // pointer data is manipulated
    *(++infoPtr) = '\0';
    // incrementing pointer data
    *record = newRec; // assigning record to new record
    return RC_OK;
    // returns success
}

//function to offset
RC attrOffset (Schema *schema, int attrNum, int *res)
{
    *res = 1;
    int i;
    //iterating through schema
    for(i = 0; i < attrNum; i++)
    {
        switch (schema->dataTypes[i]) //checking for data types in schema
        {
            case DT_INT://checking for int
                *res = *res + sizeof(int);
                //updating result accordingly
                break;
            case DT_STRING://checking for string
                *res = *res + schema->typeLength[i];
                //updating result accordingly
                break;
            case DT_BOOL://checking for bool
                *res = *res + sizeof(bool);
                //updating result accordingly
                break;
            case DT_FLOAT://checking for float
                *res = *res + sizeof(float);
                //updating result accordingly
                break;
        }
    }
    return RC_OK;
    //returns success
}

//function to free the record
extern RC freeRecord (Record *record)
{
    free(record);
    //deallocates record pointer
    return RC_OK;
    //returns success
}

//function to get attribute
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    int displacement = 0;
    attrOffset(schema, attrNum, &displacement);
    //calls attrOffset function
    Value *attr = (Value*) malloc(sizeof(Value));
    //alloacting memory to attribute pointer
    char *infoPtr = record->data;
    //updating info pointer to record data
    infoPtr = infoPtr + displacement;
    //incrementing info pointer
    int len;
    schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
    //checking for schema data types and assigining values appropriately
    switch(schema->dataTypes[attrNum]) //checks for schema datatype
    {
        // checks for string
        case DT_STRING:
        {
            len = schema->typeLength[attrNum];
            // length is assigned according to type
            attr->v.stringV = (char *) malloc(len + 1);
            // allocating memory to attribute data
            strncpy(attr->v.stringV, infoPtr, len);
            // calls string copy function with updated length
            attr->v.stringV[len] = '\0';
            attr->dt = DT_STRING;
            // attribute data is assigned to string
            break;
        }
        // checks for integer
        case DT_INT:
        {
            int value;
            value = 0;//value is set to 0
            memcpy(&value, infoPtr, sizeof(int));
            //memory copy of value to info pointer
            attr->v.intV = value;
            //value of attribute is set
            attr->dt = DT_INT;
            // attribute data is assigned to int
            break;
        }
        // checks for float
        case DT_FLOAT:
        {
            float value;
            memcpy(&value, infoPtr, sizeof(float));
            //memory copy of value to info pointer
            attr->v.floatV = value;
            //value of attribute is set 
            attr->dt = DT_FLOAT;
            // attribute data is assigned to float
            break;
        }
        // checks for boolean
        case DT_BOOL:
        {
            bool value;
            memcpy(&value,infoPtr, sizeof(bool));
            //memory copy of value to info pointer
            attr->v.boolV = value;
            //value of attribute is set 
            attr->dt = DT_BOOL;
            // attribute data is assigned to bool
            break;
        }
        // default check
        default:
            printf("For the given data type the serializer is not defined  \n");
            break;
    }
    *value = attr;
    //value pointer is set to attribute data 
    return RC_OK;
    //returns success
}
//function to set the attributes
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    int displacement; 
    displacement = 0;
    // displacement set to 0
    attrOffset(schema, attrNum, &displacement);
    // call the attrOffset function with appropriate arguments
    char *infoPtr = record->data;
    // assigning info pointer to data of record
    infoPtr = infoPtr + displacement;
    // incrementing the pointer of info with desplacement
    switch(schema->dataTypes[attrNum])
    // checking for schema datatype
    {
        // checks for string
        case DT_STRING:
        {
            int len;
            len = schema->typeLength[attrNum]; 
            // length is incremented according to type
            strncpy(infoPtr, value->v.stringV, len); // calls string copy function with updated length
            infoPtr = infoPtr + schema->typeLength[attrNum];
            // increment pointer according to type length
            break;
        }
        // checks for integer
        case DT_INT:
        {
            *(int *) infoPtr = value->v.intV; // assigning value to pointer
            infoPtr = infoPtr + sizeof(int);
            // incrementing pointer accordingly
            break;
        }
        // checks for float
        case DT_FLOAT:
        {
            *(float *) infoPtr = value->v.floatV;
            //  assigning value to pointer
            infoPtr = infoPtr + sizeof(float);
            // incrementing pointer accordingly
            break;
        }
        // checks for boolean
        case DT_BOOL:
        {
            *(bool *) infoPtr = value->v.boolV;
            // assigning value to pointer
            infoPtr = infoPtr + sizeof(bool); // incrementing pointer accordingly
            break;
        }
        // default value check
        default:
            printf("For the given datatype the serializer is not defined \n");
            break;
    }
    return RC_OK;
    //  returns success
}