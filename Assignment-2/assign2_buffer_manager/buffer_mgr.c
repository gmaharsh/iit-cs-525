#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

int buffer_size = 0;
int rear_index = 0;
int front_index =0;
int count_write = 0;
int hit = 0;
int pointer_clock = 0;
int pointer_lfu = 0;
int least_index = 0;
int least_reference = 0;
int hit_index =0;
int hit_number = 0;

typedef struct Page
{
	SM_PageHandle data; // Actual data of the page
	PageNumber pageNum; // An identification integer given to each page
	int replace_lru;   // used for LRU replacement algorithm
	int reference; 
	int dirty_bits;
	int total;  // Used by LFU algorithm to get the least frequently used page
} PageFrame;


// Defining LFU (Least Frequently Used) function
extern void LFU(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("LFU Started");
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	
	int least_index, least_reference;
	least_index = pointer_lfu;	
	
	// Interating through all the page frames in the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[least_index].total == 0)
		{
			least_index = (least_index + i) % buffer_size;
			least_reference = frame[least_index].reference;
			break;
		}
	}

	int i = (least_index + 1) % buffer_size;

	// Finding the page frame having minimum reference (i.e. it is used the least frequent) page frame
	for(int j = 0; j < buffer_size; j++)
	{
		if(frame[i].reference < least_reference)
		{
			least_index = i;
			least_reference= frame[i].reference;
		}
		i = (i + 1) % buffer_size;
	}
		
	// If page in memory has been modified (dirty_bits = 1), then write page to disk	
	if(frame[least_index].dirty_bits == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(frame[least_index].pageNum, &fh, frame[least_index].data);
		
		// Increase the count_write which records the number of writes done by the buffer manager.
		count_write++;
	}
	
	// Setting page frame's content to new page's content		
	frame[least_index].data = page->data;
	frame[least_index].pageNum = page->pageNum;
	frame[least_index].dirty_bits = page->dirty_bits;
	frame[least_index].total = page->total;
	pointer_lfu = least_index + 1;
}


// Defining LRU (Least Recently Used) function
extern void LRU(BM_BufferPool *const bm, PageFrame *page)
{	
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	

	// Interating through all the page frames in the buffer pool.
	for(int i = 0; i < buffer_size; i++)
	{
		// Finding page frame whose total = 0 i.e. no client is using that page frame.
		if(frame[i].total == 0)
		{
			hit_index = i;
			hit_number = frame[i].replace_lru;
			break;
		}
	}	

	// Finding the page frame having minimum replace_lru (i.e. it is the least recently used) page frame
	for(int i = hit_index + 1; i < buffer_size; i++)
	{
		if(frame[i].replace_lru < hit_number)
		{
			hit_index = i;
			hit_number = frame[i].replace_lru;
		}
	}

	// If page in memory has been modified (dirty_bits = 1), then write page to disk
	if(frame[hit_index].dirty_bits == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(frame[hit_index].pageNum, &fh, frame[hit_index].data);
		
		// Increase the count_write which records the number of writes done by the buffer manager.
		count_write++;
	}
	
	// Setting page frame's content to new page's content
	frame[hit_index].data = page->data;
	frame[hit_index].pageNum = page->pageNum;
	frame[hit_index].dirty_bits = page->dirty_bits;
	frame[hit_index].total = page->total;
	frame[hit_index].replace_lru = page->replace_lru;
}

extern void FIFO(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("FIFO Started");
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	
	front_index = rear_index % buffer_size;

	// Interating through all the page frames in the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[front_index].total == 0)
		{
			// If page in memory has been modified (dirty_bits = 1), then write page to disk
			if(frame[front_index].dirty_bits == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(frame[front_index].pageNum, &fh, frame[front_index].data);
				
				// Increase the count_write which records the number of writes done by the buffer manager.
				count_write++;
			}
			
			// Setting page frame's content to new page's content
			frame[front_index].data = page->data;
			frame[front_index].pageNum = page->pageNum;
			frame[front_index].dirty_bits = page->dirty_bits;
			frame[front_index].total = page->total;
			break;
		}
		else
		{
			// If the current page frame is being used by some client, we move on to the next location
			front_index++;
			front_index = (front_index % buffer_size == 0) ? 0 : front_index;
		}
	}
}
// Defining CLOCK function
extern void CLOCK(BM_BufferPool *const bm, PageFrame *page)
{	
	//printf("CLOCK Started");
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	while(1)
	{
		pointer_clock = (pointer_clock % buffer_size == 0) ? 0 : pointer_clock;

		if(frame[pointer_clock].replace_lru == 0)
		{
			// If page in memory has been modified (dirty_bits = 1), then write page to disk
			if(frame[pointer_clock].dirty_bits == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(frame[pointer_clock].pageNum, &fh, frame[pointer_clock].data);
				
				// Increase the count_write which records the number of writes done by the buffer manager.
				count_write++;
			}
			
			// Setting page frame's content to new page's content
			frame[pointer_clock].data = page->data;
			frame[pointer_clock].pageNum = page->pageNum;
			frame[pointer_clock].dirty_bits = page->dirty_bits;
			frame[pointer_clock].total = page->total;
			frame[pointer_clock].replace_lru = page->replace_lru;
			pointer_clock++;
			break;	
		}
		else
		{
			// Incrementing clockPointer so that we can check the next page frame location.
			// We set replace_lru = 0 so that this loop doesn't go into an infinite loop.
			frame[pointer_clock++].replace_lru = 0;		
		}
	}
}

// ***** BUFFER POOL FUNCTIONS ***** //

/* 
   This function creates and initializes a buffer pool with numPages page frames.
   pageFileName stores the name of the page file whose pages are being cached in memory.
   strategy represents the page replacement strategy (FIFO, LRU, LFU, CLOCK) that will be used by this buffer pool
   stratData is used to pass parameters if any to the page replacement strategy
*/
extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData)
{
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;

	// Reserver memory space = number of pages x space required for one page
	PageFrame *page = malloc(sizeof(PageFrame) * numPages);
	
	// Buffersize is the total number of pages in memory or the buffer pool.
	buffer_size = numPages;	

	// Intilalizing all pages in buffer pool. The values of fields (variables) in the page is either NULL or 0
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
	count_write = pointer_clock = pointer_lfu = 0;
	return RC_OK;
		
}

// Shutdown i.e. close the buffer pool, thereby removing all the pages from the memory and freeing up all resources and releasing some memory space.
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	// Write all dirty pages (modified pages) back to disk
	forceFlushPool(bm);
	
	for(int i = 0; i < buffer_size; i++)
	{
		// If total != 0, it means that the contents of the page was modified by some client and has not been written back to disk.
		if(frame[i].total != 0)
		{
			return RC_PINNED_PAGES_IN_BUFFER;
		}
	}

	// Releasing space occupied by the page
	free(frame);
	bm->mgmtData = NULL;
	return RC_OK;
}

// This function writes all the dirty pages (having total = 0) to disk
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	
	// Store all dirty pages (modified pages) in memory to page file on disk	
	for(int i = 0; i < buffer_size; i++)
	{
		if(frame[i].total == 0 && frame[i].dirty_bits == 1)
		{
			SM_FileHandle fh;
			// Opening page file available on disk
			openPageFile(bm->pageFile, &fh);
			// Writing block of data to the page file on disk
			writeBlock(frame[i].pageNum, &fh, frame[i].data);
			// Mark the page not dirty.
			frame[i].dirty_bits = 0;
			// Increase the count_write which records the number of writes done by the buffer manager.
			count_write++;
		}
	}	
	return RC_OK;
}


// ***** PAGE MANAGEMENT FUNCTIONS ***** //

// This function marks the page as dirty indicating that the data of the page has been modified by the client
extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	
	// Iterating through all the pages in the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		// If the current page is the page to be marked dirty, then set dirty_bits = 1 (page has been modified) for that page
		if(frame[i].pageNum == page->pageNum)
		{
			frame[i].dirty_bits = 1;
			return RC_OK;		
		}			
	}		
	return RC_ERROR;
}

// This function unpins a page from the memory i.e. removes a page from the memory
extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	
	// Iterating through all the pages in the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		// If the current page is the page to be unpinned, then decrease total (which means client has completed work on that page) and exit loop
		if(frame[i].pageNum == page->pageNum)
		{
			frame[i].total--;
			break;		
		}		
	}
	return RC_OK;
}

// This function writes the contents of the modified pages back to the page file on disk
extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	
	// Iterating through all the pages in the buffer pool
	for(int i = 0; i < buffer_size; i++)
	{
		// If the current page = page to be written to disk, then right the page to the disk using the storage manager functions
		if(frame[i].pageNum == page->pageNum)
		{		
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(frame[i].pageNum, &fh, frame[i].data);
		
			// Mark page as undirty because the modified page has been written to disk
			frame[i].dirty_bits = 0;
			
			// Increase the count_write which records the number of writes done by the buffer manager.
			count_write++;
		}
	}	
	return RC_OK;
}

// This function pins a page with page number pageNum i.e. adds the page with page number pageNum to the buffer pool.
// If the buffer pool is full, then it uses appropriate page replacement strategy to replace a page in memory with the new page being pinned. 
extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	
	// Checking if buffer pool is empty and this is the first page to be pinned
	if(frame[0].pageNum == -1)
	{
		// Reading page from disk and initializing page frame's content in the buffer pool
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		frame[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, frame[0].data);
		frame[0].pageNum = pageNum;
		frame[0].total++;
		rear_index = hit = 0;
		frame[0].replace_lru = hit;	
		frame[0].reference = 0;
		page->pageNum = pageNum;
		page->data = frame[0].data;
		
		return RC_OK;		
	}
	else
	{	
		bool isBufferFull = true;
		
		for(int i = 0; i < buffer_size; i++)
		{
			if(frame[i].pageNum != -1)
			{	
				// Checking if page is in memory
				if(frame[i].pageNum == pageNum)
				{
					// Increasing total i.e. now there is one more client accessing this page
					frame[i].total++;
					isBufferFull = false;
					hit++; // Incrementing hit (hit is used by LRU algorithm to determine the least recently used page)

					if(bm->strategy == RS_LRU)
						// LRU algorithm uses the value of hit to determine the least recently used page	
						frame[i].replace_lru = hit;
					else if(bm->strategy == RS_CLOCK)
						// replace_lru = 1 to indicate that this was the last page frame examined (added to the buffer pool)
						frame[i].replace_lru = 1;
					else if(bm->strategy == RS_LFU)
						// Incrementing reference to add one more to the count of number of times the page is used (referenced)
						frame[i].reference++;
					
					page->pageNum = pageNum;
					page->data = frame[i].data;

					pointer_clock++;
					break;
				}				
			} else {
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				frame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, frame[i].data);
				frame[i].pageNum = pageNum;
				frame[i].total = 1;
				frame[i].reference = 0;
				rear_index++;	
				hit++; // Incrementing hit (hit is used by LRU algorithm to determine the least recently used page)

				if(bm->strategy == RS_LRU)
					// LRU algorithm uses the value of hit to determine the least recently used page
					frame[i].replace_lru = hit;				
				else if(bm->strategy == RS_CLOCK)
					// replace_lru = 1 to indicate that this was the last page frame examined (added to the buffer pool)
					frame[i].replace_lru = 1;
						
				page->pageNum = pageNum;
				page->data = frame[i].data;
				
				isBufferFull = false;
				break;
			}
		}
		
		// If isBufferFull = true, then it means that the buffer is full and we must replace an existing page using page replacement strategy
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
				// LRU algorithm uses the value of hit to determine the least recently used page
				newPage->replace_lru = hit;				
			else if(bm->strategy == RS_CLOCK)
				// replace_lru = 1 to indicate that this was the last page frame examined (added to the buffer pool)
				newPage->replace_lru = 1;

			page->pageNum = pageNum;
			page->data = newPage->data;			

			// Call appropriate algorithm's function depending on the page replacement strategy selected (passed through parameters)
			switch(bm->strategy)
			{			
				case RS_FIFO: // Using FIFO algorithm
					FIFO(bm, newPage);
					break;
				
				case RS_LRU: // Using LRU algorithm
					LRU(bm, newPage);
					break;
				
				case RS_CLOCK: // Using CLOCK algorithm
					CLOCK(bm, newPage);
					break;
  				
				case RS_LFU: // Using LFU algorithm
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
		return RC_OK;
	}	
}


// ***** STATISTICS FUNCTIONS ***** //

// This function returns an array of page numbers.
extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber *frameContents = malloc(sizeof(PageNumber) * buffer_size);
	PageFrame *frame = (PageFrame *) bm->mgmtData;
	
	for(int i=0;i<buffer_size;i++){
		frameContents[i] = (frame[i].pageNum != -1) ? frame[i].pageNum : NO_PAGE;
	}
	return frameContents;
}

// This function returns an array of bools, each element represents the dirty_bits of the respective page.
extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool *dirtyFlags = malloc(sizeof(bool) * buffer_size);
	PageFrame *frame = (PageFrame *)bm->mgmtData;
	
	// Iterating through all the pages in the buffer pool and setting dirtyFlags' value to TRUE if page is dirty else FALSE
	for(int i = 0; i < buffer_size; i++)
	{
		dirtyFlags[i] = (frame[i].dirty_bits == 1) ? true : false ;
	}	
	return dirtyFlags;
}

// This function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame.
extern int *getFixCounts (BM_BufferPool *const bm)
{
	int *fixCounts = malloc(sizeof(int) * buffer_size);
	PageFrame *frame= (PageFrame *)bm->mgmtData;
	
	for(int i=0;i<buffer_size;i++){
		fixCounts[i] = (frame[i].total != -1) ? frame[i].total : 0;
	}
	return fixCounts;
}

// This function returns the number of pages that have been read from disk since a buffer pool has been initialized.
extern int getNumReadIO (BM_BufferPool *const bm)
{
	// Adding one because with start rear_index with 0.
	return (rear_index + 1);
}

// This function returns the number of pages written to the page file since the buffer pool has been initialized.
extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return count_write;
}