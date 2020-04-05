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
    printf("Nihar Darji - A20449275\n");
	pageFile = NULL;
}

//Creating a Page File in read and write mode
extern RC createPageFile (char *fileName){ 
    RC return_code;
    //the file is opened in write and append mode
    pageFile = fopen(fileName,"wb+");
    //the file pointer pagefile points to file
    if(pageFile == NULL){ //Checks the file, if value is NULL then it would return file not found message
        return_code = RC_FILE_NOT_FOUND;          
    }else{ //if the file pointer is not null 
        SM_PageHandle empty = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
        //used to initialize a block of size PAGE_SIZE
        int write = fwrite(empty,sizeof(char),PAGE_SIZE,pageFile);//writes PAGE_SIZE block of memory on to file 
        //pointed by 'empty' pagehandler 
        if (write < PAGE_SIZE || write == 0){ //checks if the value returned by fwrite is valid 
            return_code = RC_WRITE_FILE_FAILED; //since its not valid it returns write fail
        }
        else{
            return_code = RC_OK; //else returns success
        }


        fclose(pageFile); //closes filepointer
        free(empty); //used to deallocate memory
    }
    return return_code; 
}

//Open the created Page file
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    RC return_code; 
    pageFile = fopen(fileName,"r+");//opens file in read and append mode
    if(pageFile == NULL){ //checks for null condition
        return_code = RC_FILE_NOT_FOUND;//returns file not found 
    }else{
        //fseek is used to get the filepointer position 
        int totalPageNumber = fseek(pageFile, 0L, SEEK_END);//holds the integer value returned by fseek 
        //SEEK_END corresponds to end of file
        if(totalPageNumber != 0){ //checks for error condition
            return_code = RC_SEEK_FILE_POSITION_ERROR; 
        }else{
            int seek = fseek(pageFile,0L,SEEK_END);//used to seek file position 
            if(seek != 0){
                return_code = RC_SEEK_FILE_POSITION_ERROR;//checks error condition
            }

        }

        int size = ftell(pageFile); //returns the file positon with respect to the starting of the file
        //total number of pages is calculated with respect to PAGE_SIZE
        int totalNumberPages = size / PAGE_SIZE; 
        fHandle->fileName = fileName; //initializes filehander's filename
        fHandle->totalNumPages = totalNumberPages;//initializes filehander's totalNumPages
        fHandle->curPagePos = 0;//initializes filehander's curPagePos
        fHandle->mgmtInfo = pageFile;//initializes filehander's mgmtInfo

        return_code = RC_OK; //returns success
    }
    fclose(pageFile);//closes the file by deallocating the file pointer
    return return_code;//returns status code
}

//Closing the file after usage
extern RC closePageFile (SM_FileHandle *fHandle){
    if(pageFile != NULL){ //checks for null condition
        pageFile = NULL; 
    }
    return RC_OK; //returns success otherwise
}


//Removing the Page File From the disk
extern RC destroyPageFile (char *fileName){
    RC return_code;
    //opens file in read mode
    pageFile = fopen(fileName,"r");

    if (pageFile == NULL){ //checks for null condition
        return_code = RC_FILE_NOT_FOUND;//returns file not found error
    }else{
        remove(fileName); //else removes the file 
        return_code = RC_OK; //returns success
    }

    return return_code; //returns status
}

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC return_code;
    pageFile = fopen(fHandle->fileName,"r");
    //opens the file in read mode
	if (pageNum > fHandle->totalNumPages || pageNum < 0){
		return_code =  RC_READ_NON_EXISTING_PAGE; //returns non existing page error
	}else if(pageFile == NULL){ //checks for null condition
		return_code = RC_FILE_NOT_FOUND; //returns file not found error
	}else{
		int start = fseek(pageFile, (pageNum * PAGE_SIZE), SEEK_SET); //used to get the file position of the file
		if(start == 0) {
			if(fread(memPage, sizeof(char), PAGE_SIZE, pageFile) < PAGE_SIZE)
                //reads PAGE_SIZE block of data
				return_code = RC_ERROR; //returns error 
		} else {
			return_code = RC_READ_NON_EXISTING_PAGE; //returns page non existing error
		}
		fHandle->curPagePos = ftell(pageFile); //seeks the position of page handler with respect to starting
		
		fclose(pageFile);//closes file and deallocates file pointer
		
	    return_code = RC_OK;//returns success
	}

    //returns status
	return return_code;
}
	


//Get the Block Position
extern int getBlockPos (SM_FileHandle *fHandle){
    //gets the current file handler position of the block
    return fHandle->curPagePos; 
}

//Reading the first Block
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");
    //opens the file in read mode
    if(pageFile == 0){ //checks for zero condition
        return_code = RC_FILE_NOT_FOUND; //returns file not found
    }else{
        readBlock(0,fHandle,memPage); //else reads the 0th block 
        fclose(pageFile); //closes the file and deallocates the pointer
        return_code = RC_OK; //returns success
    }
    //returns the status otherwise
    return return_code;
}

//Reading the previous block
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");
    //opens the file in read mode
    if(pageFile == 0){ //checks for zero condition
        return_code =  RC_FILE_NOT_FOUND;//returns file not found
    //checks for filehandler position to get the previous block
    }else if(((fHandle)->curPagePos - 1) < 0){ //checks for negative condition
        return_code = RC_READ_NON_EXISTING_PAGE;//returns non existing page error
    }
    else{
        readBlock((getBlockPos(fHandle)-1),fHandle,memPage);//reads block is used and getblock position is used to get the 
        //position of the specified block
        return_code = RC_OK; //returns success
    }

    fclose(pageFile);//closes the file and deallocates the pointer
    //returns the status 
    return return_code;
}

//Read the current block of page file
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");
    //opens the file in read mode

    if(pageFile == 0){//checks for zero condition
        return_code =  RC_FILE_NOT_FOUND;//returns file not found
    }else{//if file is found
        readBlock(getBlockPos(fHandle),fHandle,memPage);//used to get position of the file handler for the current block
        return_code = RC_OK;//returns success
    }
    fclose(pageFile);//closes the file and deallocates the pointer
    //returns the status 
    return return_code;

}

//Read the next block
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");
    //opens the file in read mode

    if(pageFile == 0){//checks for zero condition
        return_code =  RC_FILE_NOT_FOUND;//returns file not found
    //if file is found
    }else{
        readBlock((getBlockPos(fHandle) + 1),fHandle,memPage);
        //used to get position of the file handler for the next block(current+1)
        return_code = RC_OK;//returns success
    }
    fclose(pageFile);//closes the file and deallocates the pointer
    //returns the status
    return return_code;
}

//Read the last block of page file
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    RC return_code;
    pageFile = fopen(fHandle->fileName,"r");
    //opens the file in read mode

    if(pageFile == 0){//checks for zero condition
        return_code =  RC_FILE_NOT_FOUND;//returns file not found
    }else{
        //if file is found
        readBlock((fHandle->totalNumPages),fHandle,memPage);//length is used to calculate the position of last block
        //used to get position of the file handler for the last block
        return_code = RC_OK;//returns success
    }
    fclose(pageFile);//closes the file and deallocates the pointer
    //returns the status
    return return_code;
}


extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC return_code;
	pageFile = fopen(fHandle->fileName, "r+");
    //opens the file in read mode and append mode
	if (pageNum > fHandle->totalNumPages || pageNum < 0){
		return_code = RC_WRITE_FAILED; //returns write failed error
	}else if(pageFile == NULL){ //checking for null code
		return_code = RC_FILE_NOT_FOUND; 
        //returns file not found error
	}else{
		int startPosition = pageNum * PAGE_SIZE;
        //initializes the start position
		if(pageNum == 0) { //checks for zero
			fseek(pageFile, startPosition, SEEK_SET);
            //fetches page file from startposition
			int i;
			for(i = 0; i < PAGE_SIZE; i++) 
			{
				if(feof(pageFile)) //checks for end of file
					 appendEmptyBlock(fHandle);	//appends empty block	
				fputc(memPage[i], pageFile);
			}
			fHandle->curPagePos = ftell(pageFile); 
			fclose(pageFile);	//closes file and deallocates file pointer
		} else {
            //seeks start position of file handler
			fHandle->curPagePos = startPosition;
			fclose(pageFile);//closes the file pointer
			writeCurrentBlock(fHandle, memPage);
            //writes the current block on to file
		}
		return RC_OK;	//returns success
	}
	return return_code;//returns status
}

extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC return_code;
	pageFile = fopen(fHandle->fileName, "r+");
    //opens the file in read mode and append mode
	if(pageFile == NULL){//checks for null condition
		return_code = RC_FILE_NOT_FOUND;//returns file not found error
	}else{
		appendEmptyBlock(fHandle);//appends empty block
		fseek(pageFile, fHandle->curPagePos, SEEK_SET);
        //fetches page file from startposition
		fwrite(memPage, sizeof(char), strlen(memPage), pageFile);
		fHandle->curPagePos = ftell(pageFile);
        //gets the page pointer 
		fclose(pageFile);//closes the file pointer
		return_code = RC_OK;//returns success
	}
    //returns status
	return return_code;
}


extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
	RC return_code;
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    //alloacates memory using calloc
	int app = fseek(pageFile, 0, SEEK_END);
    //fetches the file poinnter from end
	if(app == 0 ) {
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, pageFile);
        //writes PAGE_SIZE block on to emptyfile
	} else {
		free(emptyBlock);//deallocates memory
		return_code = RC_WRITE_FAILED;//returns write failed
	}
	free(emptyBlock);//dealloactes memory
	fHandle->totalNumPages++;//increments total number of pages
	return_code = RC_OK;//returns success
    //returns status
	return return_code;
}

//Ensuring the capacity of file
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    RC return_code;
    //fetches the page number
    int page_number = fHandle->totalNumPages;
    //if number of pages is greater than page number 
    if (numberOfPages > page_number){
        int add_page = numberOfPages - page_number; //finds the block to append
        for (int i = 0; i < add_page; i++)
            appendEmptyBlock(fHandle);
            //appends an empty block pointed by the file handler
        return_code = RC_OK; //returns success
    }else{
        //returns write failed error
        return_code = RC_WRITE_FAILED;
    }
    //returns status
    return return_code;

}