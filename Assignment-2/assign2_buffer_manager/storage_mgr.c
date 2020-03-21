#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "storage_mgr.h"

FILE *pageFile;

extern void initStorageManager (void) {
	printf("Initializing the storage manager\n");
    printf("Made by:-\n");
    printf("Maharsh Hetal Gheewala - A20442506\n");
    printf("Nihar Darji - A2044\n");
	pageFile = NULL;
}

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

extern RC closePageFile (SM_FileHandle *fHandle) {
	if(pageFile != NULL){
        pageFile = NULL;
    }
    return RC_OK;
}


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

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC return_code;
    pageFile = fopen(fHandle->fileName,"r");
    
	if (pageNum > fHandle->totalNumPages || pageNum < 0){
		return_code =  RC_READ_NON_EXISTING_PAGE;
	}else if(pageFile == NULL){
		return_code = RC_FILE_NOT_FOUND;
	}else{
		int start = fseek(pageFile, (pageNum * PAGE_SIZE), SEEK_SET);
		if(start == 0) {
			if(fread(memPage, sizeof(char), PAGE_SIZE, pageFile) < PAGE_SIZE)
				return_code = RC_ERROR;
		} else {
			return_code = RC_READ_NON_EXISTING_PAGE; 
		}
		fHandle->curPagePos = ftell(pageFile); 
		
		fclose(pageFile);
		
	    return_code = RC_OK;
	}
	return return_code;
}
	


extern int getBlockPos (SM_FileHandle *fHandle) {
	// Returning the current page position retrieved from the file handle	
	return fHandle->curPagePos;
}

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

    return return_code;
}

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

extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC return_code;
	pageFile = fopen(fHandle->fileName, "r+");
	if (pageNum > fHandle->totalNumPages || pageNum < 0){
		return_code = RC_WRITE_FAILED;
	}else if(pageFile == NULL){
		return_code = RC_FILE_NOT_FOUND;
	}else{
		int startPosition = pageNum * PAGE_SIZE;

		if(pageNum == 0) { 
			fseek(pageFile, startPosition, SEEK_SET);	
			int i;
			for(i = 0; i < PAGE_SIZE; i++) 
			{
				if(feof(pageFile))
					 appendEmptyBlock(fHandle);			
				fputc(memPage[i], pageFile);
			}
			fHandle->curPagePos = ftell(pageFile); 
			fclose(pageFile);	
		} else {
			fHandle->curPagePos = startPosition;
			fclose(pageFile);
			writeCurrentBlock(fHandle, memPage);
		}
		return RC_OK;	
	}
	return return_code;
}

extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC return_code;
	pageFile = fopen(fHandle->fileName, "r+");
	if(pageFile == NULL){
		return_code = RC_FILE_NOT_FOUND;
	}else{
		appendEmptyBlock(fHandle);
		fseek(pageFile, fHandle->curPagePos, SEEK_SET);
		fwrite(memPage, sizeof(char), strlen(memPage), pageFile);
		fHandle->curPagePos = ftell(pageFile);
		fclose(pageFile);
		return_code = RC_OK;
	}
	return return_code;
}


extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
	RC return_code;
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	int app = fseek(pageFile, 0, SEEK_END);
	if(app == 0 ) {
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, pageFile);
	} else {
		free(emptyBlock);
		return_code = RC_WRITE_FAILED;
	}
	free(emptyBlock);
	fHandle->totalNumPages++;
	return_code = RC_OK;
	return return_code;
}

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