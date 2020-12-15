#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include "dberror.h"
#include "storage_mgr.h"

FILE *pageFile; //file pointer

//Intializing the storage manager
extern void initStorageManager (void){
	printf("Initializing the storage manager\n");
	printf("Made by:-\n");
	printf("Maharsh Hetal Gheewala - A20442506\n");
	printf("Nihar Darji - A2044\n");
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

//Reading a Block from a file
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code; 
	RC read_size;
	//used to hold read size 

	pageFile = fopen(fHandle->fileName,"r");
	//opens file in read mode
	if(pageFile == 0){ //checking if the value is zero
		return_code = RC_FILE_NOT_FOUND; //returns file not found error
	}else if(fHandle->totalNumPages < pageNum){ //checks if totalpages pointed by filehandler< page number 
		return_code = RC_READ_NON_EXISTING_PAGE; //returns error
	}else{
		int start =  fseek(pageFile, pageNum * PAGE_SIZE, SEEK_SET); //used to get the file position of the file
		if(start != 0){ //checks for error condition
			return_code = RC_ERROR;
		}
		//used to read PAGE_SIZE amount of block from mempage
		read_size = fread(memPage, sizeof(char), PAGE_SIZE, pageFile);
		if (read_size < PAGE_SIZE || read_size > PAGE_SIZE) {
			return_code = RC_READ_NON_EXISTING_PAGE; //checks for non existing page condition
		}
		//used to set the current position of the file handler
		fHandle->curPagePos = pageNum;
		return_code = RC_OK; //returns success
	}
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


//Writing a block to file
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code;
	pageFile = fopen(fHandle->fileName,"r+");
	//opens the file in read mode and append mode
	if(pageFile == NULL){//checks for null condition
		return_code = RC_FILE_NOT_FOUND;//returns file not found error
	}else if(pageNum > fHandle->totalNumPages || pageNum <0){
		//checks if totalpages pointed by filehandler< page number and if page number <0
		return_code = RC_WRITE_FAILED;//returns write failed error
	}else{
		if(pageFile != NULL){ //checks for null condition
			//to get the position of file handler fseek is used 
			if(fseek(pageFile, pageNum*PAGE_SIZE,SEEK_SET) == 0){
				//checks for zero condition
				fwrite(memPage,sizeof(char),strlen(memPage),pageFile);//writes mempage size of bytes on to pagefile
				//initializes the filehandler's current positon
				fHandle->curPagePos = pageNum;//to page number
				fclose(pageFile);//closes pagefile
				return_code = RC_OK;//returns success
			}else{
				//returns write failed error
				return_code = RC_WRITE_FAILED;
			}
		}else{
			//else returns file not found error
			return_code = RC_FILE_NOT_FOUND;
		}
	}
	//returns status
	return return_code;
}


//Write the current Block to file
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code;
	int current_position = getBlockPos(fHandle);
	//fetches the current position of the filehandler
	fHandle->totalNumPages ++; //increments the total number of pages count
	int current_block = writeBlock(current_position,fHandle,memPage);
	//used to write a block and returns back an int value
	if(current_block == RC_OK){//if success
		return_code = RC_OK; //returns the same
	}else{
		//else returns write failed error
		return_code = RC_WRITE_FAILED;
	}

	//returns status
	return return_code;
}

//Write in empty file
extern RC appendEmptyBlock (SM_FileHandle *fHandle){
	RC return_code;
	pageFile = fopen(fHandle->fileName,"r+");
	//opens page in read and append mode
	if(pageFile == NULL){ //checks for null condition
		return_code = RC_FILE_NOT_FOUND; //returns file not found error
	}else{
		//else initialize total pages
		int totalPages = fHandle->totalNumPages;
  		fHandle->totalNumPages ++; //increments the total number of pages
  		fseek(pageFile,totalPages*PAGE_SIZE,SEEK_SET);//fetches the file handler position 
  		char c = 0;
  		int i;
		//to write a block of size PAGE_SIZE
  		for(i=0;i<PAGE_SIZE;i++){
    		fwrite(&c,sizeof(c),1,pageFile); //of size of bytes of c
  		}
  		return_code = RC_OK;//returns success
	}
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