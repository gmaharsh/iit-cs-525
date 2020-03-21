#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

// This structure represents one page frame in buffer pool (memory).
typedef struct Page
{
	SM_PageHandle data; // Actual data of the page
	PageNumber pageNum; // An identification integer given to each page
	int count_total = t; // number of clients using a page at the given instancex
	int dirty_bits; //page modification indicator
	int lru_replace;   // used for LRU replacement algorithm
	int num_ref;   // Used by LFU algorithm to get the least frequently used page
} PageFrame;

// "size_buffer" represents the size of the buffer pool i.e. maximum number of page frames that can be kept into the buffer pool
int size_buffer = 0;

// "index_rear" basically stores the count of number of pages read from the disk.
// "rearIndex" is also used by FIFO function to calculate the frontIndex i.e.
int index_rear = 0;

// "writeCount" counts the number of I/O write to the disk i.e. number of pages writen to the disk
int writeCount = 0;

// "hit" a general count which is incremented whenever a page frame is added into the buffer pool.
// "hit" is used by LRU to determine least recently added page into the buffer pool.
int hit = 0;

// "clockPointer" is used by CLOCK algorithm to point to the last added page in the buffer pool.
int clockPointer = 0;

// "lfuPointer" is used by LFU algorithm to store the least frequently used page frame's position. It speeds up operation  from 2nd replacement onwards.
int lfuPointer = 0;

// Defining FIFO (First In First Out) function
extern void FIFO(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("FIFO Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i, frontIndex;
	frontIndex = index_rear % size_buffer;

	// Interating through all the page frames in the buffer pool
	for(i = 0; i < size_buffer; i++)
	{
		if(pageFrame[frontIndex].count_total == 0)
		{
			// If page in memory has been modified (dirty_bits = 1), then write page to disk
			if(pageFrame[frontIndex].dirty_bits == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[frontIndex].pageNum, &fh, pageFrame[frontIndex].data);
				
				// Increase the writeCount which records the number of writes done by the buffer manager.
				writeCount++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[frontIndex].data = page->data;
			pageFrame[frontIndex].pageNum = page->pageNum;
			pageFrame[frontIndex].dirty_bits = page->dirty_bits;
			pageFrame[frontIndex].count_total = page->count_total;
			break;
		}
		else
		{
			// If the current page frame is being used by some client, we move on to the next location
			frontIndex++;
			frontIndex = (frontIndex % size_buffer == 0) ? 0 : frontIndex;
		}
	}
}

// Defining LFU (Least Frequently Used) function
extern void LFU(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("LFU Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i, j, leastFreqIndex, leastFreqRef;
	leastFreqIndex = lfuPointer;	
	
	// Interating through all the page frames in the buffer pool
	for(i = 0; i < size_buffer; i++)
	{
		if(pageFrame[leastFreqIndex].count_total == 0)
		{
			leastFreqIndex = (leastFreqIndex + i) % size_buffer;
			leastFreqRef = pageFrame[leastFreqIndex].num_ref;
			break;
		}
	}

	i = (leastFreqIndex + 1) % size_buffer;

	// Finding the page frame having minimum num_ref (i.e. it is used the least frequent) page frame
	for(j = 0; j < size_buffer; j++)
	{
		if(pageFrame[i].num_ref < leastFreqRef)
		{
			leastFreqIndex = i;
			leastFreqRef = pageFrame[i].num_ref;
		}
		i = (i + 1) % size_buffer;
	}
		
	// If page in memory has been modified (dirty_bits = 1), then write page to disk	
	if(pageFrame[leastFreqIndex].dirty_bits == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastFreqIndex].pageNum, &fh, pageFrame[leastFreqIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCount++;
	}
	
	// Setting page frame's content to new page's content		
	pageFrame[leastFreqIndex].data = page->data;
	pageFrame[leastFreqIndex].pageNum = page->pageNum;
	pageFrame[leastFreqIndex].dirty_bits = page->dirty_bits;
	pageFrame[leastFreqIndex].count_total = page->count_total;
	lfuPointer = leastFreqIndex + 1;
}

// Defining LRU (Least Recently Used) function
extern void LRU(BM_BufferPool *const bm, PageFrame *page)
{	
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	int i, leastHitIndex, leastHitNum;

	// Interating through all the page frames in the buffer pool.
	for(i = 0; i < size_buffer; i++)
	{
		// Finding page frame whose count_total = 0 i.e. no client is using that page frame.
		if(pageFrame[i].count_total == 0)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].lru_replace;
			break;
		}
	}	

	// Finding the page frame having minimum lru_replace (i.e. it is the least recently used) page frame
	for(i = leastHitIndex + 1; i < size_buffer; i++)
	{
		if(pageFrame[i].lru_replace < leastHitNum)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].lru_replace;
		}
	}

	// If page in memory has been modified (dirty_bits = 1), then write page to disk
	if(pageFrame[leastHitIndex].dirty_bits == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastHitIndex].pageNum, &fh, pageFrame[leastHitIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCount++;
	}
	
	// Setting page frame's content to new page's content
	pageFrame[leastHitIndex].data = page->data;
	pageFrame[leastHitIndex].pageNum = page->pageNum;
	pageFrame[leastHitIndex].dirty_bits = page->dirty_bits;
	pageFrame[leastHitIndex].count_total = page->count_total;
	pageFrame[leastHitIndex].lru_replace = page->lru_replace;
}

// Defining CLOCK function
extern void CLOCK(BM_BufferPool *const bm, PageFrame *page)
{	
	//printf("CLOCK Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	while(1)
	{
		clockPointer = (clockPointer % size_buffer == 0) ? 0 : clockPointer;

		if(pageFrame[clockPointer].lru_replace == 0)
		{
			// If page in memory has been modified (dirty_bits = 1), then write page to disk
			if(pageFrame[clockPointer].dirty_bits == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[clockPointer].pageNum, &fh, pageFrame[clockPointer].data);
				
				// Increase the writeCount which records the number of writes done by the buffer manager.
				writeCount++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[clockPointer].data = page->data;
			pageFrame[clockPointer].pageNum = page->pageNum;
			pageFrame[clockPointer].dirty_bits = page->dirty_bits;
			pageFrame[clockPointer].count_total = page->count_total;
			pageFrame[clockPointer].lru_replace = page->lru_replace;
			clockPointer++;
			break;	
		}
		else
		{
			// Incrementing clockPointer so that we can check the next page frame location.
			// We set lru_replace = 0 so that this loop doesn't go into an infinite loop.
			pageFrame[clockPointer++].lru_replace = 0;		
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
	size_buffer = numPages;	
	int i;

	// Intilalizing all pages in buffer pool. The values of fields (variables) in the page is either NULL or 0
	for(i = 0; i < size_buffer; i++)
	{
		page[i].data = NULL;
		page[i].pageNum = -1;
		page[i].dirty_bits = 0;
		page[i].count_total = 0;
		page[i].lru_replace = 0;	
		page[i].num_ref = 0;
	}

	bm->mgmtData = page;
	writeCount = clockPointer = lfuPointer = 0;
	return RC_OK;
		
}

// Shutdown i.e. close the buffer pool, thereby removing all the pages from the memory and freeing up all resources and releasing some memory space.
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	// Write all dirty pages (modified pages) back to disk
	forceFlushPool(bm);

	int i;	
	for(i = 0; i < size_buffer; i++)
	{
		// If count_total != 0, it means that the contents of the page was modified by some client and has not been written back to disk.
		if(pageFrame[i].count_total != 0)
		{
			return RC_PINNED_PAGES_IN_BUFFER;
		}
	}

	// Releasing space occupied by the page
	free(pageFrame);
	bm->mgmtData = NULL;
	return RC_OK;
}

// This function writes all the dirty pages (having count_total = 0) to disk
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Store all dirty pages (modified pages) in memory to page file on disk	
	for(i = 0; i < size_buffer; i++)
	{
		if(pageFrame[i].count_total == 0 && pageFrame[i].dirty_bits == 1)
		{
			SM_FileHandle fh;
			// Opening page file available on disk
			openPageFile(bm->pageFile, &fh);
			// Writing block of data to the page file on disk
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].data);
			// Mark the page not dirty.
			pageFrame[i].dirty_bits = 0;
			// Increase the writeCount which records the number of writes done by the buffer manager.
			writeCount++;
		}
	}	
	return RC_OK;
}


// ***** PAGE MANAGEMENT FUNCTIONS ***** //

// This function marks the page as dirty indicating that the data of the page has been modified by the client
extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Iterating through all the pages in the buffer pool
	for(i = 0; i < size_buffer; i++)
	{
		// If the current page is the page to be marked dirty, then set dirty_bits = 1 (page has been modified) for that page
		if(pageFrame[i].pageNum == page->pageNum)
		{
			pageFrame[i].dirty_bits = 1;
			return RC_OK;		
		}			
	}		
	return RC_ERROR;
}

// This function unpins a page from the memory i.e. removes a page from the memory
extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Iterating through all the pages in the buffer pool
	for(i = 0; i < size_buffer; i++)
	{
		// If the current page is the page to be unpinned, then decrease count_total (which means client has completed work on that page) and exit loop
		if(pageFrame[i].pageNum == page->pageNum)
		{
			pageFrame[i].count_total--;
			break;		
		}		
	}
	return RC_OK;
}

// This function writes the contents of the modified pages back to the page file on disk
extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Iterating through all the pages in the buffer pool
	for(i = 0; i < size_buffer; i++)
	{
		// If the current page = page to be written to disk, then right the page to the disk using the storage manager functions
		if(pageFrame[i].pageNum == page->pageNum)
		{		
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].data);
		
			// Mark page as undirty because the modified page has been written to disk
			pageFrame[i].dirty_bits = 0;
			
			// Increase the writeCount which records the number of writes done by the buffer manager.
			writeCount++;
		}
	}	
	return RC_OK;
}

// This function pins a page with page number pageNum i.e. adds the page with page number pageNum to the buffer pool.
// If the buffer pool is full, then it uses appropriate page replacement strategy to replace a page in memory with the new page being pinned. 
extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	// Checking if buffer pool is empty and this is the first page to be pinned
	if(pageFrame[0].pageNum == -1)
	{
		// Reading page from disk and initializing page frame's content in the buffer pool
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		pageFrame[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, pageFrame[0].data);
		pageFrame[0].pageNum = pageNum;
		pageFrame[0].count_total++;
		index_rear = hit = 0;
		pageFrame[0].lru_replace = hit;	
		pageFrame[0].num_ref = 0;
		page->pageNum = pageNum;
		page->data = pageFrame[0].data;
		
		return RC_OK;		
	}
	else
	{	
		int i;
		bool isBufferFull = true;
		
		for(i = 0; i < size_buffer; i++)
		{
			if(pageFrame[i].pageNum != -1)
			{	
				// Checking if page is in memory
				if(pageFrame[i].pageNum == pageNum)
				{
					// Increasing count_total i.e. now there is one more client accessing this page
					pageFrame[i].count_total++;
					isBufferFull = false;
					hit++; // Incrementing hit (hit is used by LRU algorithm to determine the least recently used page)

					if(bm->strategy == RS_LRU)
						// LRU algorithm uses the value of hit to determine the least recently used page	
						pageFrame[i].lru_replace = hit;
					else if(bm->strategy == RS_CLOCK)
						// lru_replace = 1 to indicate that this was the last page frame examined (added to the buffer pool)
						pageFrame[i].lru_replace = 1;
					else if(bm->strategy == RS_LFU)
						// Incrementing num_ref to add one more to the count of number of times the page is used (referenced)
						pageFrame[i].num_ref++;
					
					page->pageNum = pageNum;
					page->data = pageFrame[i].data;

					clockPointer++;
					break;
				}				
			} else {
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				pageFrame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, pageFrame[i].data);
				pageFrame[i].pageNum = pageNum;
				pageFrame[i].count_total = 1;
				pageFrame[i].num_ref = 0;
				index_rear++;	
				hit++; // Incrementing hit (hit is used by LRU algorithm to determine the least recently used page)

				if(bm->strategy == RS_LRU)
					// LRU algorithm uses the value of hit to determine the least recently used page
					pageFrame[i].lru_replace = hit;				
				else if(bm->strategy == RS_CLOCK)
					// lru_replace = 1 to indicate that this was the last page frame examined (added to the buffer pool)
					pageFrame[i].lru_replace = 1;
						
				page->pageNum = pageNum;
				page->data = pageFrame[i].data;
				
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
			newPage->count_total = 1;
			newPage->num_ref = 0;
			index_rear++;
			hit++;

			if(bm->strategy == RS_LRU)
				// LRU algorithm uses the value of hit to determine the least recently used page
				newPage->lru_replace = hit;				
			else if(bm->strategy == RS_CLOCK)
				// lru_replace = 1 to indicate that this was the last page frame examined (added to the buffer pool)
				newPage->lru_replace = 1;

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
	PageNumber *frameContents = malloc(sizeof(PageNumber) * size_buffer);
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i = 0;
	// Iterating through all the pages in the buffer pool and setting frameContents' value to pageNum of the page
	while(i < size_buffer) {
		frameContents[i] = (pageFrame[i].pageNum != -1) ? pageFrame[i].pageNum : NO_PAGE;
		i++;
	}
	return frameContents;
}

// This function returns an array of bools, each element represents the dirty_bits of the respective page.
extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool *dirtyFlags = malloc(sizeof(bool) * size_buffer);
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Iterating through all the pages in the buffer pool and setting dirtyFlags' value to TRUE if page is dirty else FALSE
	for(i = 0; i < size_buffer; i++)
	{
		dirtyFlags[i] = (pageFrame[i].dirty_bits == 1) ? true : false ;
	}	
	return dirtyFlags;
}

// This function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame.
extern int *getFixCounts (BM_BufferPool *const bm)
{
	int *fixCounts = malloc(sizeof(int) * size_buffer);
	PageFrame *pageFrame= (PageFrame *)bm->mgmtData;
	
	int i = 0;
	// Iterating through all the pages in the buffer pool and setting fixCounts' value to page's fixCount
	while(i < size_buffer)
	{
		fixCounts[i] = (pageFrame[i].count_total != -1) ? pageFrame[i].count_total : 0;
		i++;
	}	
	return fixCounts;
}

// This function returns the number of pages that have been read from disk since a buffer pool has been initialized.
extern int getNumReadIO (BM_BufferPool *const bm)
{
	// Adding one because with start index_rear with 0.
	return (index_rear + 1);
}

// This function returns the number of pages written to the page file since the buffer pool has been initialized.
extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return writeCount;
}