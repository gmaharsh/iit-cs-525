#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

int buffer_size = 0;//refers to the buffer size
int rear_index = 0;
int front_index =0;
int least_index = 0;
int hit_index =0;
int least_reference = 0;//used for lru algorithm 
int count_write = 0;
int hit = 0;
int pointer_clock = 0; //used for clock algorithm
int pointer_lfu = 0; //used for lfu algorithm 
int hit_number = 0; //gets the frquency of hits

//typedef used for pageframe 
typedef struct Page
{
	SM_PageHandle data; //initializing the page handler
	PageNumber pageNum; //gives the page number count
	int replace_lru;   // used for LRU replacement algorithm
	int reference; 
	int dirty_bits;//used to refer to dirty bits
	int total;  //used for maintaining the total page count
} PageFrame;


//LFU (Least Frequently Used) function
extern void LFU(BM_BufferPool *const bm, PageFrame *page)
{
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	//initializing the page frame
	int least_index, least_reference;
	least_index = pointer_lfu;
	//page frames are looped over and initialized
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[least_index].total == 0)
		{
			least_index = (least_index + i) % buffer_size;
			least_reference = frame[least_index].reference;
			break;
		}
	}
	//buffer size and least_index are used to refer to the page frame
	int i = (least_index + 1) % buffer_size;
	//Used to find the least recently used page 
	for(int j = 0; j < buffer_size; j++)
	{
		if(frame[i].reference < least_reference)//page frame conpared with the least reference
		{
			least_index = i;
			least_reference= frame[i].reference;// least reference and index are assigned respectively
		}
		i = (i + 1) % buffer_size;// i is incremented according to buffer size
	}
		

	//dirty bit is checked, if it is equal to 1 then it is written on to page
	if(frame[least_index].dirty_bits == 1)
	{
		//file handler is initialized
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);//open file page function is called with the file pointer
		writeBlock(frame[least_index].pageNum, &fh, frame[least_index].data);//write block function is used 
		//to write data on to block with page frame reference
		count_write++;//is incremented when write is done
	}
	//new page frame is initialized with the existing page data
	frame[least_index].data = page->data; //page data is initialized
	frame[least_index].pageNum = page->pageNum;//page num is initialized
	frame[least_index].dirty_bits = page->dirty_bits;//dirty bits are initialized
	frame[least_index].total = page->total;//total pages are initialized
	pointer_lfu = least_index + 1;//pointer is incremented with respect to the index
}

//LRU (Least Recently Used) function
extern void LRU(BM_BufferPool *const bm, PageFrame *page)
{	
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	//page frames are initialized 
	//data in buffer is iterated 
	for(int i = 0; i < buffer_size; i++) //the data is iterated till buffer_size is reached
	{
		if(frame[i].total == 0)//to check if the page frame is already used by other 
		{
			hit_index = i; //hit index is initalized with index
			hit_number = frame[i].replace_lru; //hit number is initialized with the replaced with page frame
			break;
		}
	}
	//used to find the least replaced page frame
	for(int i = hit_index + 1; i < buffer_size; i++)
	{
		//condition is checked if the pages replaced are less than the hit number
		if(frame[i].replace_lru < hit_number)
		{
			hit_index = i; //hit index is initalized with index
			hit_number = frame[i].replace_lru;//hit number is initialized with the replaced with page frame
		}
	}
	//page in memory is modified then dirty bit is initialized to 1
	if(frame[hit_index].dirty_bits == 1)
	{
		SM_FileHandle fh;//page file handler
		openPageFile(bm->pageFile, &fh);//open page file with initialized page handler
		writeBlock(frame[hit_index].pageNum, &fh, frame[hit_index].data);//write block is used to 
		// write data on to block with page frame reference
		count_write++;//is incremented when write is done
	}
	
	//new page frame is initialized with the existing page data 
	frame[hit_index].data = page->data;//page data is initialized
	frame[hit_index].pageNum = page->pageNum;//page num is initialized
	frame[hit_index].dirty_bits = page->dirty_bits;//dirty bits are initialized
	frame[hit_index].total = page->total;//total pages are initialized
	frame[hit_index].replace_lru = page->replace_lru;//pointer is incremented with respect to the index
}

extern void FIFO(BM_BufferPool *const bm, PageFrame *page)
{
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	//page frame is intialized
	front_index = rear_index % buffer_size;//index is referenced using rear index and buffer size
	//iterating through the buffer pool 
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[front_index].total == 0)//used to check if the page frame is already in use
		{
			if(frame[front_index].dirty_bits == 1)//to check page frame is modified
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);//open page file with file pointer
				writeBlock(frame[front_index].pageNum, &fh, frame[front_index].data);
				//data is written on to page
				count_write++;//write count is incremented
			}
			//page frame is initialized
			frame[front_index].data = page->data;//page data is initialized
			frame[front_index].pageNum = page->pageNum;//page num is initialized
			frame[front_index].dirty_bits = page->dirty_bits;//dirty bits are initialized
			frame[front_index].total = page->total;//total pages are initialized
			break;
		}
		else
		{
			front_index++;//increment the page frame on another page if not found in current
			front_index = (front_index % buffer_size == 0) ? 0 : front_index;//index is initialized depending 
			//on the buffer_size
		}
	}
}
//CLOCK function
extern void CLOCK(BM_BufferPool *const bm, PageFrame *page)
{	
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	//page frame is intialized
	while(1)
	{
		//pointer clock is initialized depending on buffer size
		pointer_clock = (pointer_clock % buffer_size == 0) ? 0 : pointer_clock;
		if(frame[pointer_clock].replace_lru == 0)//is checked for replace lru
		{
			if(frame[pointer_clock].dirty_bits == 1)//to check if it is modified
			{
				SM_FileHandle fh;//page frame
				openPageFile(bm->pageFile, &fh);//open page file with file pointer
				writeBlock(frame[pointer_clock].pageNum, &fh, frame[pointer_clock].data);
				//write block on to page
				//write is incremented
				count_write++;
			}
			//page frame is intialized with new page frame
			frame[pointer_clock].data = page->data;//page data is initialized
			frame[pointer_clock].pageNum = page->pageNum;//page num is initialized
			frame[pointer_clock].dirty_bits = page->dirty_bits;//dirty bits are initialized
			frame[pointer_clock].total = page->total;//total pages are initialized
			frame[pointer_clock].replace_lru = page->replace_lru;
			pointer_clock++;//pointer clock is incremented
			break;	
		}
		else
		{
			frame[pointer_clock++].replace_lru = 0;//its initalized to 0 so that it does not enter an infinite loop
		}
	}
}

//BUFFER POOL FUNCTIONS

extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData)
{
	//initializing buffer pool
	bm->pageFile = (char *)pageFileName;//page file is initialized
	bm->numPages = numPages;
	bm->strategy = strategy;
	//page is allocated memeory using malloc
	PageFrame *page = malloc(sizeof(PageFrame) * numPages);
	//buffer size is assigned to number of pages
	buffer_size = numPages;	
	//pages in buffer are initialized
	for(int i = 0; i < buffer_size; i++)
	{
		page[i].data = NULL;
		page[i].pageNum = -1;
		page[i].dirty_bits = 0;
		page[i].total = 0;
		page[i].replace_lru = 0;	
		page[i].reference = 0;
	}
	bm->mgmtData = page;
	count_write = pointer_clock = pointer_lfu = 0;//write count is initialized to 0
	return RC_OK; //return success
}

//shut down buffer pool function
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	//frame is initialized
	forceFlushPool(bm);//flush buffer pool 
	//initializing the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		//checks if the page is used by other client
		if(frame[i].total != 0)
		{
			return RC_PINNED_PAGES_IN_BUFFER;//returns status
		}
	}
	//deallocates memory
	free(frame);
	bm->mgmtData = NULL;
	return RC_OK;//returns success
}
//pushes dirty pages to disk
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	//page frame is initialized 
	//buffer page is initialized
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[i].total == 0 && frame[i].dirty_bits == 1)//checks dirty bits and used pages
		{
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);//opens the page file 
			writeBlock(frame[i].pageNum, &fh, frame[i].data);//opens the block and writes
			frame[i].dirty_bits = 0;//the page is marked not dirty
			count_write++;//write count is incremented
		}
	}	
	return RC_OK;//returns success
}


// ***** PAGE MANAGEMENT FUNCTIONS ***** //

//mark dirty 
extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	//page is initialized
	// Iterating through all the pages in the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[i].pageNum == page->pageNum)
		{
			frame[i].dirty_bits = 1;//dirty bit is set to 1 indicating page is modified
			return RC_OK;//returns success		
		}			
	}		
	return RC_ERROR;//returns error
}

//unpin page
extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	//page frame is initialized
	// Iterating through all the pages in the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[i].pageNum == page->pageNum)//page number is checked
		{
			frame[i].total--;//to unpin page decrement the count of total
			break;		
		}		
	}
	return RC_OK;//returns success
}

//forcepage
extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	//page frame is initialized
	// Iterating through all the pages in the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[i].pageNum == page->pageNum)//page number and frame number is checked
		{		
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);//page file is opened
			writeBlock(frame[i].pageNum, &fh, frame[i].data);
			//data is written on to file
			frame[i].dirty_bits = 0;//dirty bit is marked as 0 indicating not modified
			count_write++;//write count is incremented
		}
	}	
	return RC_OK;//returns success
}
//pin page
extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	//page frame is initialized
	if(frame[0].pageNum == -1)//checking if page frame is empty
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);//used to open page frame
		frame[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		//memory is allocated to the page framedata
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, frame[0].data);//reads data from the page
		frame[0].pageNum = pageNum;//initialize the page number
		frame[0].total++;
		rear_index = hit = 0;
		frame[0].replace_lru = hit;	
		frame[0].reference = 0;
		page->pageNum = pageNum;//page number is assigned to new page frame number
		page->data = frame[0].data;//page data is assigned to new page frame data
		
		return RC_OK;		
	}
	else
	{	
		//to check if buffer is full
		bool isBufferFull = true;
		//iterating through buffer pool
		for(int i = 0; i < buffer_size; i++)
		{
			if(frame[i].pageNum != -1)
			{	
				if(frame[i].pageNum == pageNum)// Checking if page is in memory
				{
					frame[i].total++;//checks if page is used by other clients
					isBufferFull = false;//buffer full is marked false
					hit++;//hit is incremented

					if(bm->strategy == RS_LRU)	
						frame[i].replace_lru = hit;//hit value is used to check for least recently used frame
					else if(bm->strategy == RS_CLOCK)
						//clock algorithm is called
						frame[i].replace_lru = 1;
					else if(bm->strategy == RS_LFU)
						frame[i].reference++;//counts number of times page is used
					
					page->pageNum = pageNum;
					page->data = frame[i].data;
					pointer_clock++;//clock pointer is incremented
					break;
				}				
			} else {
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);//used to open page frame
				frame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
				//memory is allocated to the page framedata
				readBlock(pageNum, &fh, frame[i].data);//reads data from page
				frame[i].pageNum = pageNum;
				frame[i].total = 1;
				frame[i].reference = 0;
				rear_index++;//increments rear index	
				hit++; //increments hit

				if(bm->strategy == RS_LRU)
					// LRU algorithm 
					frame[i].replace_lru = hit;				
				else if(bm->strategy == RS_CLOCK)
					// clock algorithm
					frame[i].replace_lru = 1;
						
				page->pageNum = pageNum;
				page->data = frame[i].data;
				
				isBufferFull = false;//buffer full is marked false
				break;
			}
		}
		
		if(isBufferFull == true)
		{
			// Create a new page to store data read from the file.
			PageFrame *newPage = (PageFrame *) malloc(sizeof(PageFrame));		
			
			// Reading page from disk and initializing page frame's content in the buffer pool
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			newPage->data = (SM_PageHandle) malloc(PAGE_SIZE);
			readBlock(pageNum, &fh, newPage->data);
			newPage->pageNum = pageNum;
			newPage->dirty_bits = 0;		
			newPage->total = 1;
			newPage->reference = 0;
			rear_index++;
			hit++;

			if(bm->strategy == RS_LRU)
				// LRU algorithm
				newPage->replace_lru = hit;				
			else if(bm->strategy == RS_CLOCK)
				// replace_lru = 1 to indicate that this was the last page frame examined (added to the buffer pool)
				newPage->replace_lru = 1;

			page->pageNum = pageNum;
			page->data = newPage->data;			
			//used to call the algorithm appropriately
			switch(bm->strategy)
			{			
				case RS_FIFO: // FIFO algorithm
					FIFO(bm, newPage);
					break;
				
				case RS_LRU: //  LRU algorithm
					LRU(bm, newPage);
					break;
				
				case RS_CLOCK: // CLOCK algorithm
					CLOCK(bm, newPage);
					break;
  				
				case RS_LFU: // LFU algorithm
					LFU(bm, newPage);
					break;
  				
				case RS_LRU_K:
					printf("\n LRU-k algorithm not implemented");
					break;
				
				default:
					printf("\nAlgorithm Not Implemented\n");
					break;
			}
						
		}		
		return RC_OK;//returns success
	}	
}


//STATISTICS FUNCTIONS 

extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber *frameContents = malloc(sizeof(PageNumber) * buffer_size);//frame content is initialized
	PageFrame *frame = (PageFrame *) bm->mgmtData;//frame is initialized
	//iterating through buffer pool
	for(int i=0;i<buffer_size;i++){
		frameContents[i] = (frame[i].pageNum != -1) ? frame[i].pageNum : NO_PAGE;
	}
	return frameContents;//frame content is returned
}

extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	//dirtyflag is initialized
	bool *dirtyFlags = malloc(sizeof(bool) * buffer_size);
	PageFrame *frame = (PageFrame *)bm->mgmtData;//page frame is initialized
	for(int i = 0; i < buffer_size; i++)
	{
		dirtyFlags[i] = (frame[i].dirty_bits == 1) ? true : false ;//dirty flag is marked for every page in a buffer
	}	
	return dirtyFlags;
}

extern int *getFixCounts (BM_BufferPool *const bm)
{
	int *fixCounts = malloc(sizeof(int) * buffer_size);//count is initialized
	PageFrame *frame= (PageFrame *)bm->mgmtData;//page frame is initialized
	
	for(int i=0;i<buffer_size;i++){
		fixCounts[i] = (frame[i].total != -1) ? frame[i].total : 0;//count is marked for every page in buffer
	}
	return fixCounts;
}

extern int getNumReadIO (BM_BufferPool *const bm)
{
	return (rear_index + 1);
}

extern int getNumWriteIO (BM_BufferPool *const bm)
{
	//returns write count
	return count_write;
}