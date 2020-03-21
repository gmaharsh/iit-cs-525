#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"

FILE *pageFile;

/*  FUNCTION NAME : initStorageManager
    DESCRIPTION   : Initialize the storage manager */

extern void initStorageManager (void) {
    printf("Initializing the storage manager\n");
    printf("Made by:-\n");
    printf("Maharsh Hetal Gheewala - A20442506\n");
    printf("Nihar Darji - A2044\n");
}

/* FUNCTION NAME : createPageFile
   DESCRIPTION   : Creates a new page file in append mode and write to it */

extern RC createPageFile (char *fileName) {

    RC return_code;
    pageFile = fopen(fileName,"wb+");

    if(pageFile == NULL){ //Checks the file, if value is NULL then it would return file not found message
        return_code = RC_FILE_NOT_FOUND;          
    }else{
        SM_PageHandle empty = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));

        int write = fwrite(empty,sizeof(char),PAGE_SIZE,pageFile);

        if (write < PAGE_SIZE || write == 0){
            return_code = RC_WRITE_FILE_FAILED;
        }
        else{
            return_code = RC_OK;
        }

        fclose(pageFile);
        free(empty);
    }
    return return_code;
}

/* FUNCTION NAME : openPageFile
   DESCRIPTION   : Opens the created file in read mode to store informantion */ 

extern RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
    
    RC return_code;
    pageFile = fopen(fileName,"r+");

    if(pageFile == NULL){
        return_code = RC_FILE_NOT_FOUND;
    }else{

        int totalPageNumber = fseek(pageFile, 0L, SEEK_END);

        if(totalPageNumber != 0){
            return_code = RC_SEEK_FILE_POSITION_ERROR;
        }else{
            int seek = fseek(pageFile,0L,SEEK_END);
            if(seek != 0){
                return_code = RC_SEEK_FILE_POSITION_ERROR;
            }

        }

        int size = ftell(pageFile);
        int totalNumberPages = size / PAGE_SIZE;
        fHandle->fileName = fileName;
        fHandle->totalNumPages = totalNumberPages;
        fHandle->curPagePos = 0;
        fHandle->mgmtInfo = pageFile;

        return_code = RC_OK;
    }
    fclose(pageFile);
    return return_code;
}

/* FUNCTION NAME : closePageFile
   DESCRIPTION   : closes the opened file */

extern RC closePageFile (SM_FileHandle *fHandle) {
    if(pageFile != NULL)
        pageFile = NULL;    
    return RC_OK; 
}

/* FUNCTION NAME : destroyPageFile 
   DESCRIPTION   : Deletes the page file  */

extern RC destroyPageFile (char *fileName) {    
    RC return_code;

    pageFile = fopen(fileName,"r");

    if (pageFile == NULL){
        return_code = RC_FILE_NOT_FOUND;
    }else{
        remove(fileName);
        return_code = RC_OK;
    }

    return return_code;
}

/* FUNCTION NAME : readBlock
   DESCRIPTION   : this function will read the pageNum-th block of data */

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC return_code;
    RC read_size;
    pageFile = fopen(fHandle->fileName,"r");

    if(pageFile == 0){
        return_code = RC_FILE_NOT_FOUND; 
    }else if(fHandle->totalNumPages < pageNum){
        return_code = RC_READ_NON_EXISTING_PAGE;
    }else{
        int start =  fseek(pageFile, pageNum * PAGE_SIZE, SEEK_SET);
        if(start != 0){
            return_code = RC_ERROR;
        }
        read_size = fread(memPage, sizeof(char), PAGE_SIZE, pageFile);
        if (read_size < PAGE_SIZE || read_size > PAGE_SIZE) {
            return_code = RC_READ_NON_EXISTING_PAGE;
        }
        fHandle->curPagePos = pageNum;
        return_code = RC_OK;
    }
    return return_code;
}

/* FUNCTION NAME : getBlockPos
   DESCRIPTION   : this function returns the current block position */

extern int getBlockPos (SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

/* FUNCTION NAME : readFirstBlock
   DESCRIPTION   : reads the first block of data from page file */

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {  
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");

    if(pageFile == 0){
        return_code = RC_FILE_NOT_FOUND;
    }else{
        readBlock(0,fHandle,memPage);
        fclose(pageFile);
        return_code = RC_OK;
    }

    return return_code
}

/* FUNCTION NAME : readPreviousBlock
   DESCRIPTION   : reads the page from previous block */

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");

    if(pageFile == 0){
        return_code =  RC_FILE_NOT_FOUND;
    }else if(((fHandle)->curPagePos - 1) < 0){
        return_code = RC_READ_NON_EXISTING_PAGE;
    }
    else{
        readBlock((getBlockPos(fHandle)-1),fHandle,memPage);
        return_code = RC_OK;
    }

    fclose(pageFile);
    return return_code;
}

/* FUNCTION NAME : readCurrentBlock
   DESCRIPTION   : reads the page from current block */

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");

    if(pageFile == 0){
        return_code =  RC_FILE_NOT_FOUND;
    }else{
        readBlock(getBlockPos(fHandle),fHandle,memPage);
        return_code = RC_OK;
    }
    fclose(pageFile);
    return return_code;   
}

/* FUNCTION NAME : readNextBlock
   DESCRIPTION   : reads the page from next block */

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");

    if(pageFile == 0){
        return_code =  RC_FILE_NOT_FOUND;
    }else{
        readBlock((getBlockPos(fHandle) + 1),fHandle,memPage);
        return_code = RC_OK;
    }
    fclose(pageFile);
    return return_code;
}

/* FUNCTION NAME : readLastBlock
   DESCRIPTION   :  read the page from last block */

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");

    if(pageFile == 0){
        return_code =  RC_FILE_NOT_FOUND;
    }else{
        readBlock((fHandle->totalNumPages),fHandle,memPage);
        return_code = RC_OK;
    }
    fclose(pageFile);
    return return_code;
}

/* FUNCTION NAME : writeBlock 
   DESCRIPTION   : Writes the pageNum-th block of data */

extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r+");

    if(pageFile == NULL){
        return_code = RC_FILE_NOT_FOUND;
    }else if(pageNum > fHandle->totalNumPages || pageNum <0){
        return_code = RC_WRITE_FAILED;
    }else{
        if(pageFile != NULL){
            if(fseek(pageFile, pageNum*PAGE_SIZE,SEEK_SET) == 0){
                fwrite(memPage,sizeof(char),strlen(memPage),pageFile);
                fHandle->curPagePos = pageNum;
                fclose(pageFile);
                return_code = RC_OK;
            }else{
                return_code = RC_WRITE_FAILED;
            }
        }else{
            return_code = RC_FILE_NOT_FOUND;
        }
    }
}

/* FUNCTION NAME : writeCurrentBlock
   DESCRIPTION   : writes page to the current block */


extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    RC return_code;
    int current_position = getBlockPos(fHandle);

    fHandle->totalNumPages ++;
    int current_block = writeBlock(current_position,fHandle,memPage);

    if(current_block == RC_OK){
        return_code = RC_OK;
    }else{
        return_code = RC_WRITE_FAILED;
    }

    return return_code;}

/* FUNCTION NAME : appendEmptyBlock
   DESCRIPTION   : write an empty page to the file by appending at the end */

extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r+");

    if(pageFile == NULL){
        return_code = RC_FILE_NOT_FOUND;
    }else{
        int totalPages = fHandle->totalNumPages;
        fHandle->totalNumPages ++;
        fseek(pageFile,totalPages*PAGE_SIZE,SEEK_SET);
        char c = 0;
        int i;
        for(i=0;i<PAGE_SIZE;i++){
            fwrite(&c,sizeof(c),1,pageFile);
        }
        return_code = RC_OK;
    }
    return return_code;
}

/* FUNCTION NAME : ensureCapacity
   DESCRIPTION   : if the file has less number of pages than totalNumPages then increase the size */

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
    RC return_code;
    
    int page_number = fHandle->totalNumPages;
    if (numberOfPages > page_number){
        int add_page = numberOfPages - page_number;
        for (int i = 0; i < add_page; i++)
            appendEmptyBlock(fHandle);

        return_code = RC_OK;
    }else{
        return_code = RC_WRITE_FAILED;
    }

    return return_code;
}