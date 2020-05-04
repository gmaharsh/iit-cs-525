#include <stdio.h>
#include "dberror.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <sys/stat.h>
#include "buffer_mgr.h"
#include "tables.h"
#include "dt.h"
#include <string.h>

// structure for accessing particular node
typedef struct Node {
	struct Node * next;
	struct Node * ptNode;
	bool isLeaf;
	int nodeKeys;
	void ** ptrKy;
	Value ** curKy;
} Node;

// structure for accessing the data
typedef struct Data {
	RID recId;
} Data;

//More information storage for B+ tree
typedef struct BTMgr {
	BM_BufferPool BP;
	BM_PageHandle pghand;
	int mxchld;
	int numnodes;
	int nument;
	Node * Root_node;
	Node * prtseq ;
	DataType datkey;
} BTMgr;

//To make the scan operation easier, we keep a track of following data
typedef struct Scan_info {
	int keyindx;
	int numKys;
	int mxchld;
	Node * nodeSpec;
} Scan_info;


// Additional functions used to carry out B+ tree operations smoothly
Node * root_changes(Node * Root_node);
Node * combineNode(BTMgr * BTMng, Node * node, Node * L_node, int L_ind, Value * value);
Node * leafSearch(Node * r_node, Value * key);
Data * keySearch(Node * r_node, Value *key);
Node * removeByKey(BTMgr * BTMng, Value * key);
bool isEqual(Value * key1, Value * key2);
Node * removeNodeKey(BTMgr * BTMng, Node * node, Value * key, Node * ptr);
void seqInsert(BTMgr * BTMng, Node * node);
Node * getSeq(BTMgr * BTMng);
int reachRoot(Node * r_node, Node * ch_node);
Node * newNode(BTMgr * BTMng);
int getNeighbourInfo(Node * node);
Node * newLeaf(BTMgr * BTMng);
Node * insertLeafAfterSplit(BTMgr * BTMng, Node * leaf_n, Value * key, Data * ptr);
bool isGreater(Value * key1, Value * key2);
Node * insertNodeAfterSplit(BTMgr * BTMng, Node * prv_node, int LHS_ind , Value * key, Node * RHS);
Node * parentInsert(BTMgr * BTMng, Node * LHS, Value * key, Node * RHS);
Node * keyRedistibution(Node * Root_node, Node * node, Node * L_node, int L_ind, int ind, Value * value);
Node * removeKey(BTMgr * BTMng, Node * node, Value * key, void * ptr);
bool isLess(Value * key1, Value * key2);


//Initialize B+tree
BTMgr * BTMng = NULL;

//Initailize Index manager
RC initIndexManager(void *mgmtData) {
	//Initialize Storage manager
	initStorageManager();
	return RC_OK;
}

//Shutdown index manager
RC shutdownIndexManager() {
	BTMng = NULL;
	return RC_OK;
}

void BTmgrAssignment(DataType typeOfKey, int n){
	BTMng->mxchld = n + 2;	// Setting order of B+ Tree
	BTMng->numnodes = 0;	// No nodes initially.
	BTMng->nument = 0;	// No entries initially
	BTMng->Root_node = NULL; // No root node
	BTMng->prtseq = NULL; // No node for printing
	BTMng->datkey = typeOfKey; // Set datatype to "keyType"
}

//Create a new B+tree
RC createBtree(char *idxId, DataType typeOfKey, int n){

	int order = PAGE_SIZE / sizeof(Node);
	if (n <= order) {
        BTMng = (BTMgr *) calloc(1,sizeof(BTMgr)); // Retrieve B+ Tree handle and assign metadata structure
        BM_BufferPool *buffmg = (BM_BufferPool *) calloc(1,sizeof(BM_BufferPool));
        BTMng->BP = *buffmg;
        SM_FileHandle fh;
        char actual_data[PAGE_SIZE];
		BTmgrAssignment(typeOfKey, n);
        int statret;
        statret = createPageFile(idxId);
        if(statret != RC_OK)
            return statret;
        statret = openPageFile(idxId, &fh);
        if(statret != RC_OK)
            return statret;
        statret = writeBlock(0, &fh, actual_data);
        if(statret != RC_OK)
            return statret;
        statret = closePageFile(&fh);
        if(statret != RC_OK)
            return statret;
        return (RC_OK);
	}
    else
	    return RC_UNABLE_TO_ACCOMODATE_THIS_ORDER_TREE;
}

void assignTreeMgmtData(BTreeHandle **tree ){
	*tree = (BTreeHandle *) calloc(1,sizeof(BTreeHandle));
	(*tree)->mgmtData = BTMng;
}

//Open the created B+tree
RC openBtree(BTreeHandle **tree, char *idxId)
{
	assignTreeMgmtData(tree);
    int statret = initBufferPool(&BTMng->BP, idxId, MAX_PAGES, RS_LRU, NULL);
        if(statret != RC_OK)
            return statret;
    return (RC_OK);
}

//Close the B+tree
RC closeBtree(BTreeHandle *tree)
{
	BTMgr * BTMng = (BTMgr*) tree->mgmtData;
	markDirty(&BTMng->BP, &BTMng->pghand); // marking the page as dirty
	shutdownBufferPool(&BTMng->BP);
	free(BTMng);
	free(tree); // release memory space
	return (RC_OK);
}

//Delete the existing B+tree
RC deleteBtree(char *idxId) {
    int statret = destroyPageFile(idxId);
        if(statret != RC_OK)
            return statret;
    return (RC_OK);
}

void manupNode(Node * node, Data * newRecord,  Value *key,int maxChildrenNode ){
	node->ptNode = NULL;
	node->ptrKy[0] = newRecord;
	node->curKy[0] = key;
	node->ptrKy[maxChildrenNode - 1] = NULL;
	node->nodeKeys =node->nodeKeys + 1;

	BTMng->nument = BTMng->nument + 1;
	BTMng->Root_node = node;
}

void  manuIfLeafNode(Node * lfNode, Data * newRecord,  Value *key ){
	BTMng->nument = BTMng->nument + 1;

	int insPosition = 0;
	while (insPosition < lfNode->nodeKeys && isLess(lfNode->curKy[insPosition], key))
		insPosition++;

	int i = lfNode->nodeKeys;
	while (i > insPosition) {
		lfNode->ptrKy[i] = lfNode->ptrKy[i - 1];
		lfNode->curKy[i] = lfNode->curKy[i - 1];
		i--;
	}
	lfNode->ptrKy[insPosition] = newRecord;
	lfNode->curKy[insPosition] = key;
	lfNode->nodeKeys = lfNode->nodeKeys +1 ;
}
// Insert new key to the tree
RC insertKey(BTreeHandle *tree, Value *key, RID recId) {
	BTMgr *BTMng = (BTMgr *) tree->mgmtData;
	Node * lfNode;

	int maxChildrenNode = BTMng->mxchld;

	if (keySearch(BTMng->Root_node, key) == NULL) // verify if record with that key already exists
		Data * recnew = (Data *) calloc(1,sizeof(Data));
		if (recnew != NULL) {
			recnew->recId.page = recId.page;
			recnew->recId.slot = recId.slot;
		}
		else {
			return RC_ERROR;
		}

		if (BTMng->Root_node == NULL) {
			Node * node = newLeaf(BTMng);
			int maxChildrenNode = BTMng->mxchld;

			manupNode(node, recnew, key, maxChildrenNode);

			return RC_OK;
		}

		lfNode = leafSearch(BTMng->Root_node, key);
		if (lfNode->nodeKeys < maxChildrenNode - 1) {
			manuIfLeafNode(lfNode, recnew, key);
		}
		else {
			BTMng->Root_node = insertLeafAfterSplit(BTMng, lfNode, key, recnew);
		}
	return RC_OK;
	}
	else {
		return RC_ERROR;
	}
}

// Find the specified key in th tree
extern RC findKey(BTreeHandle *tree, Value *key, RID *result)
 {
	BTMgr *BTMng = (BTMgr *) tree->mgmtData;
	Data * key_specified = keySearch(BTMng->Root_node, key);
	if (key_specified != NULL) { // if key doesnot in tree
		*result = key_specified->recId;
		return RC_OK;
	}
	else {
		return RC_IM_KEY_NOT_FOUND;
	}
}

//Get the number of nodes in the B+ tree
RC getNumNodes(BTreeHandle *tree, int *result) {
	BTMgr * BTMng = (BTMgr *) tree->mgmtData;
	*result = BTMng->numnodes; // output stored in result parameter
	return RC_OK;
}

// Get the number of entries in the B+ tree
RC getNumEntries(BTreeHandle *tree, int *result) {
	BTMgr * BTMng = (BTMgr *) tree->mgmtData;
	*result = BTMng->nument;  // storing the result
	return RC_OK;
}

// Get the key datatype in the B+ tree
RC getKeyType(BTreeHandle *tree, DataType *result) {
	BTMgr * BTMng = (BTMgr *) tree->mgmtData;
	*result = BTMng->datkey;
	return RC_OK;
}

//Delete's the specified key
RC deleteKey(BTreeHandle *tree, Value *key) {
	BTMgr *BTMng = (BTMgr *) tree->mgmtData;
	BTMng->Root_node = removeByKey(BTMng, key); //deleting the entry
	return RC_OK;
}

// This function initializes the scan which is used to scan the entries in the B+ Tree.
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
	BTMgr *BTMng = (BTMgr *) tree->mgmtData;
	Scan_info *scanmeta = calloc(1,sizeof(Scan_info));
	*handle = calloc(1,sizeof(BT_ScanHandle)); //allocating the memory

	Node * new_node = BTMng->Root_node;
	if (BTMng->Root_node != NULL)
	{
		while (!new_node->isLeaf)
			new_node = new_node->ptrKy[0];
		scanmeta->keyindx = 0;
		scanmeta->numKys = new_node->nodeKeys;
		scanmeta->nodeSpec = new_node;
		scanmeta->mxchld = BTMng->mxchld;
		(*handle)->mgmtData = scanmeta;
	} else {
		return RC_ERROR;
	}
	return RC_OK;
}

//Finds the next entry in B+ tree
RC nextEntry(BT_ScanHandle *handle, RID *result)
{	//Set mgmtdata for the tree
	Scan_info * scanmeta = (Scan_info *) handle->mgmtData;

	//Set the parameters
	int keyindx = scanmeta->keyindx;//fetching the info
	int numKys = scanmeta->numKys;
	int treeOdr = scanmeta->mxchld;
	RID recId;

	Node * new_node = scanmeta->nodeSpec;

	// Check if the node is empty or not
	if (new_node != NULL)
	{
		//If it is not empty then scan
		if (keyindx < numKys)
		{	//If we have the next key on the same node
			recId = ((Data *) new_node->ptrKy[keyindx])->recId;
			scanmeta->keyindx++;
		}
		else
		{
			// If the next key is not present on the same node then we go to next node
			if (new_node->ptrKy[treeOdr - 1] != NULL)
			{	// We scan the next node to find the next entry
				new_node = new_node->ptrKy[treeOdr - 1];
				scanmeta->nodeSpec = new_node;
				scanmeta->keyindx = 1;
				scanmeta->numKys = new_node->nodeKeys;
				recId = ((Data *) new_node->ptrKy[0])->recId;
			}
			else
			{
				//When there are no more entries we say so
				return RC_IM_NO_MORE_ENTRIES;
			}
		}
	}
	else
	{
		return RC_IM_NO_MORE_ENTRIES;
	}
	//Store the next record id
	*result = recId;
	return RC_OK;
}

//Close the scan
extern RC closeTreeScan(BT_ScanHandle *handle) {
	handle->mgmtData = NULL;
	//Free the memory occupied by scan
	free(handle);
	return RC_OK;
}

//Print B+ tree created
extern char *printTree(BTreeHandle *tree)
{ //Set mgmtdata for the tree
	BTMgr *BTMng = (BTMgr *) tree->mgmtData;
	Node * new_node = NULL;

	if (BTMng->Root_node != NULL) {
		printf("\n B+ tree created is as follows : \n");
		BTMng->prtseq = NULL;
		//insert the seq for printing
		seqInsert(BTMng, BTMng->Root_node);
		int nr = 0;
		int r = 0;
		while (BTMng->prtseq != NULL){
			//Get the sequence for printing
			new_node = getSeq(BTMng);
			if (new_node->ptNode != NULL && new_node == new_node->ptNode->ptrKy[0]) {
				nr = reachRoot(BTMng->Root_node, new_node);
				if (nr != r) {
					r = nr;
					printf("\n");
				}
			}
			//Find the datatype and print
			int i = 0;
			while(i < new_node->nodeKeys) {
				switch (BTMng->datkey) {
				case DT_INT:
					printf("%d ", (*new_node->curKy[i]).v.intV);
					i++;
					break;
				case DT_STRING:
					printf("%s ", (*new_node->curKy[i]).v.stringV);
					i++;
					break;
				case DT_BOOL:
					printf("%d ", (*new_node->curKy[i]).v.boolV);
					i++;
					break;
				case DT_FLOAT:
					printf("%.02f ", (*new_node->curKy[i]).v.floatV);
					i++;
					break;
				}
				printf("(%d - %d) ", ((Data *) new_node->ptrKy[i])->recId.page, ((Data *) new_node->ptrKy[i])->recId.slot);
			}
			//Checkif it is a leaf node or not
			if (!new_node->isLeaf) {
				int i=0;
				while (i <= new_node->nodeKeys)
					seqInsert(BTMng, new_node->ptrKy[i]);
					i++;
			}
			printf("| ");
		}
		printf("\n");
	}
	else
		return '\0';
	//At the end we return a null value
	return '\0';
}

// Insertion

void manageLeafNode(Node * leafNode, Value ** t_keys, void ** t_pointers, int i, int j){
	leafNode->curKy[j] = t_keys[i];
	leafNode->ptrKy[j] = t_pointers[i];
	leafNode->nodeKeys++;
}

void changeLeaf(Node * leaf_n, Node * leafNode, Value ** t_keys, void ** t_pointers, int maxChildrenNode, int i){
	free(t_pointers);
	free(t_keys);
	int n = (maxChildrenNode - 1);
	leafNode->ptrKy[n] = leaf_n->ptrKy[n];
	leaf_n->ptrKy[n] = leafNode;

	for (i = leaf_n->nodeKeys; i < n; i++)
		leaf_n->ptrKy[i] = NULL;

	for (i = leafNode->nodeKeys; i < n; i++)
		leafNode->ptrKy[i] = NULL;

	leafNode->ptNode = leaf_n->ptNode;
}
// Handles insertion when it's time to split the leaf node
Node * insertLeafAfterSplit(BTMgr * BTMng, Node * leaf_n, Value * key, Data * ptr) {

	Node * leafNode;
	leafNode = newLeaf(BTMng);
	int maxChildrenNode = BTMng->mxchld;

	Value ** t_keys;
	t_keys = calloc(1,maxChildrenNode * sizeof(Value));

	void ** t_pointers;
	t_pointers = calloc(1,maxChildrenNode * sizeof(void *));
	if (t_keys == NULL || t_pointers == NULL)
	{
		return RC_ERROR;
	}

	int insert_pos = 0;
	while (insert_pos < maxChildrenNode - 1 && isLess(leaf_n->curKy[insert_pos], key))
		insert_pos++;

	int i,j;
	for (i = 0, j = 0; i < leaf_n->nodeKeys; i++, j++) {
		if (j == insert_pos)
			j++;
		t_keys[j] = leaf_n->curKy[i];
		t_pointers[j] = leaf_n->ptrKy[i];
	}

	t_keys[insert_pos] = key;
	t_pointers[insert_pos] = ptr;

	leaf_n->nodeKeys = 0;

	int split_val,m;
	m = (maxChildrenNode - 1);
	if ( m % 2 == 0)
		split_val = m / 2;
	else
		split_val = (m / 2) + 1;

	for (i = 0; i < split_val; i++) {
		leaf_n->curKy[i] = t_keys[i];
		leaf_n->ptrKy[i] = t_pointers[i];
		leaf_n->nodeKeys++;
	}

	for (i = split_val, j = 0; i < maxChildrenNode; i++, j++) {
		manageLeafNode(leafNode, t_keys, t_pointers, i, j);
	}
	changeLeaf(leaf_n, leafNode, t_keys, t_pointers, maxChildrenNode, i);

	Value * new_key = leafNode->curKy[0];

	BTMng->nument = BTMng->nument +1;

	Node * insLeafSplt = parentInsert(BTMng, leaf_n, new_key, leafNode);
	return insLeafSplt;
}

void freeNodes(Node * n_node, Node * prv_node, Value ** t_keys, void ** t_pointers, int i ){
	free(t_pointers);
	free(t_keys);

	n_node->ptNode = prv_node->ptNode;

	Node * cNode;
	for (i = 0; i <= n_node->nodeKeys; i++)
	{
		cNode = n_node->ptrKy[i];
		cNode->ptNode = n_node;
	}

	BTMng->nument = BTMng->nument +1;
}

// Handles insertion when it's time to split a node
Node * insertNodeAfterSplit(BTMgr * BTMng, Node * prv_node, int leftInd , Value * key, Node * RHS)
{

	Node * n_node;
	int maxChildrenNode = BTMng->mxchld;

	//Temporary keys and ptrs
	Value ** t_keys;
	t_keys = calloc(1,maxChildrenNode * sizeof(Value));

	void ** t_pointers;
	t_pointers = calloc(1,maxChildrenNode * sizeof(void *));
	if (t_keys == NULL || t_pointers == NULL)
	{
		return RC_ERROR;
	}
	int i,j;
	for (i = 0, j = 0; i < prv_node->nodeKeys + 1; i++, j++)
	{
		if (j == leftInd + 1)
			j++;
		t_pointers[j] = prv_node->ptrKy[i];
	}

	for (i = 0, j = 0; i < prv_node->nodeKeys; i++, j++)
	{
		if (j == leftInd)
			j++;
		t_keys[j] = prv_node->curKy[i];
	}

	t_pointers[leftInd + 1] = RHS;
	t_keys[leftInd] = key;

	//Actuall splitting takes place
	int split_val,m;
	m = (maxChildrenNode - 1);
	if ( m % 2 == 0)
		split_val = m / 2;
	else
		split_val = (m / 2) + 1;

	//Create a node
	n_node = newNode(BTMng);
	prv_node->nodeKeys = 0;
	for (i = 0; i < split_val - 1; i++)
	{
		prv_node->curKy[i] = t_keys[i];
		prv_node->ptrKy[i] = t_pointers[i];
		prv_node->nodeKeys++;
	}
	prv_node->ptrKy[i] = t_pointers[i];
	Value * val = t_keys[split_val - 1];
	for (++i, j = 0; i < maxChildrenNode ; i++, j++)
	{
		n_node->curKy[j] = t_keys[i];
		n_node->ptrKy[j] = t_pointers[i];
		n_node->nodeKeys++;
	}
	n_node->ptrKy[j] = t_pointers[i];
	freeNodes(n_node, prv_node, t_keys, t_pointers, i);
	Node * insNodeSplt = parentInsert(BTMng, prv_node, val , n_node);
	return insNodeSplt;
}

Node * retParentNode(Node * LHS, Value * key, Node * RHS) {
	Node * parent = newNode(BTMng);
	parent->ptrKy[0] = LHS;
	parent->ptrKy[1] = RHS;
	parent->curKy[0] = key;
	parent->nodeKeys = parent->nodeKeys +1 ;
	parent->ptNode = NULL;
	LHS->ptNode = parent;
	RHS->ptNode = parent;

	return parent;
}

// This function inserts a new node to the tree
Node * parentInsert(BTMgr * BTMng, Node * LHS, Value * key, Node * RHS) {

	int left_ind;
	Node * parent = LHS->ptNode;

	int maxChildrenNode = BTMng->mxchld;
	if (parent != NULL){
		int l = 0;
		while (l <= parent->nodeKeys && parent->ptrKy[l] != LHS)
		{
			l++;
		}
		left_ind = l;
		if (parent->nodeKeys < maxChildrenNode - 1) {
			int i = parent->nodeKeys;
			while ( i > left_ind) {
				parent->ptrKy[i + 1] = parent->ptrKy[i];
				parent->curKy[i] = parent->curKy[i - 1];
				i--;
			}

			parent->curKy[left_ind] = key;
			parent->ptrKy[left_ind + 1] = RHS;
			parent->nodeKeys = parent->nodeKeys + 1;

			return BTMng->Root_node;
		}

		return insertNodeAfterSplit(BTMng, parent, left_ind, key, RHS);

	}
	else{
		return retParentNode(LHS, key, RHS);
	}
}

Node * assignNodeForNewNode(int maxChildrenNode){
	Node * node = calloc(1,sizeof(Node));
	node->curKy = calloc(1,(maxChildrenNode - 1) * sizeof(Value *));
	node->ptrKy = calloc(1,maxChildrenNode * sizeof(void *));
	if (node == NULL || node->curKy == NULL || node->ptrKy == NULL) {
		return RC_ERROR;
	}
	node->nodeKeys = 0;
	node->isLeaf = false;
	node->next = NULL;
	node->ptNode = NULL;

	return node;
}

//Creat a new node, we can use this function whenever we need to create a new node
Node * newNode(BTMgr * BTMng)
{
	BTMng->numnodes = BTMng->numnodes +1 ;
	int maxChildrenNode = BTMng->mxchld;
	return assignNodeForNewNode(maxChildrenNode);
}

// Creates a new leaf by creating a node.
Node * newLeaf(BTMgr * BTMng) {
	Node * newLeaf = newNode(BTMng);
	newLeaf->isLeaf = true;
	return newLeaf;
}

//This function can search a key anywhere between root and leaf.
Node * leafSearch(Node * r_node, Value * key) {
	Node * slider = r_node;
	if (slider != NULL) {
		while (!slider->isLeaf) {
			int i = 0;
			while (i < slider->nodeKeys) {
				if (isGreater(key, slider->curKy[i]) || isEqual(key, slider->curKy[i])) {
					i++;
				} else{
					break;
				}
			}
			slider = (Node *) slider->ptrKy[i];
		}
		return slider;
	}
	else
		return slider;

}

// Searchs for the key
Data * keySearch(Node * r_node, Value *key) {
	int i = 0;
	Node * leaf_node = leafSearch(r_node, key);
	if (leaf_node != NULL)
	{
		while (i < leaf_node->nodeKeys) {
		if (isEqual(leaf_node->curKy[i], key))
			break;
		i++;
		}
		if (i != leaf_node->nodeKeys)
			return (Data *) leaf_node->ptrKy[i];
		else
			return NULL;
	}
	else {
		return NULL;
	}
}

//Gives us information for the node which is adjacent to the given node (Left node)
int getNeighbourInfo(Node * n) {
	int i=0;
	int m;
	while(i <= n->ptNode->nodeKeys) {// LOOP STARTS
		if (n->ptNode->ptrKy[i] == n) {
			m = i-1;
			return m;
		}
		i=i+1;
	}
	return RC_ERROR;
}

void ifCheckForRemoveKey(Node * n, int maxChildrenNode){
	n->nodeKeys = n->nodeKeys -1;
	BTMng->nument = BTMng->nument -1;

	if (n->isLeaf) {
		int j = n->nodeKeys;
		while(j< maxChildrenNode - 1) {
			n->ptrKy[j] = NULL;
			j=j+1;
		}
	}
	else {
		int j = n->nodeKeys + 1;
		while(j < maxChildrenNode) {
			n->ptrKy[j] = NULL;
			j=j+1;
		}
	}
}

//Deletes the specified key from the node
Node * removeNodeKey(BTMgr * BTMng, Node * n, Value * key, Node * ptr)
{

	int maxChildrenNode = BTMng->mxchld;
	int i=0;
	while (!isEqual(n->curKy[i], key)){
		i=i+1;
	}
	for (++i; i < n->nodeKeys; i++)
		n->curKy[i - 1] = n->curKy[i];

	int num_pointers;
	if(num_pointers = n->isLeaf ) {
		num_pointers = n->nodeKeys;
	}
	else {
		num_pointers = n->nodeKeys + 1;
	}

	i = 0;
	while (n->ptrKy[i] != ptr) {
		i=i+1;
	}
	for (++i; i < num_pointers; i++)
		n->ptrKy[i - 1] = n->ptrKy[i];
	ifCheckForRemoveKey(n, maxChildrenNode);
	return n;
}

//When the root has to be changed due to deletion of key
Node * root_changes(Node * r_node)
{
	Node * n;
	if (r_node->nodeKeys > 0)
		return r_node;
	else {
		if (r_node->isLeaf) {
			n = NULL;
		}
		else {
			n = r_node->ptrKy[0];
			n->ptNode = NULL;
		}
		free(r_node);
		free(r_node->curKy);
		free(r_node->ptrKy);
		return n;
	}
}


void ifLeaf(Node * node, Node * L_node, int L_ind_insert, int maxChildrenNode) {
	int i, j;
		for (i = L_ind_insert, j = 0; j < node->nodeKeys; i++, j++)
		{
			L_node->curKy[i] = node->curKy[j];
			L_node->ptrKy[i] = node->ptrKy[j];
			L_node->nodeKeys = L_node->nodeKeys +1;
		}
		int a = maxChildrenNode -1;
		L_node->ptrKy[a] = node->ptrKy[a];
}

void ifNotLeaf(Node * node, Node * L_node, Value * value, Node * t, int L_ind_insert){
	L_node->curKy[L_ind_insert] = value;
		L_node->nodeKeys = L_node->nodeKeys;
		int last = node->nodeKeys;
		int i, j;
		for (i = L_ind_insert+1; j < last;i++) {
			for(j = 0; j<last; j++) {
				L_node->curKy[i] = node->curKy[j];
				L_node->ptrKy[i] = node->ptrKy[j];
				L_node->nodeKeys = L_node->nodeKeys-1 ;
				node->nodeKeys = node->nodeKeys-1;
			}
		}

		L_node->ptrKy[i] = node->ptrKy[j];
		for (i = 0; i < L_node->nodeKeys + 1; i++) {
			t = (Node *) L_node->ptrKy[i];
			t->ptNode = L_node;
		}
}
//Merge the keys of different nodes into one node when a node has less keys than the specified minimum keys.
Node * combineNode(BTMgr * BTMng, Node * node, Node * L_node, int L_ind, Value * value)
{
	Node * t;
	int maxChildrenNode = BTMng->mxchld;
	if (L_ind == -1) {
		t = node;
		node = L_node;
		L_node = t;
	}
	int L_ind_insert = L_node->nodeKeys;
	if (!node->isLeaf) {
		ifNotLeaf(node, L_node,value, t, L_ind_insert);
	} else {
		ifLeaf(node, L_node, L_ind_insert, maxChildrenNode);
	}
	BTMng->Root_node = removeKey(BTMng, node->ptNode, value, node);
	//Free the space occupied by node
	free(node);
	free(node->curKy);
	free(node->ptrKy); // free space
	return BTMng->Root_node;
}

Node * setValueForNode(Node * node, Node * L_node, int L_ind, int ind, int space){
	Value * val = node->ptNode->curKy[ind]; // gets value of node
	Node * x;
	if (L_node->nodeKeys + node->nodeKeys < space) {
		x = combineNode(BTMng, node, L_node, L_ind, val);
		return x;
	}
	else {
		x = keyRedistibution(BTMng->Root_node, node, L_node, L_ind, ind, val);
		return x;
	}
}
int setSpaceAvailable(Node * node, int maxChildrenNode){
	int space;
	if(space == node->isLeaf) {
		space = maxChildrenNode;
	}
	else{
		space= maxChildrenNode - 1;
	}
	return space;
}

int setMinKeys(Node * node, int a ){
	int minKeys; // initialize min keys
	if (!node->isLeaf) {
		if ((a) % 2 == 0)
			minKeys = (a) / 2;
		else
			minKeys = (a / 2)+ 1;
			minKeys = minKeys -1;
	}
	else {
		if ((a - 1) % 2 == 0)
			minKeys = (a - 1) / 2;
		else
			minKeys = ((a - 1) / 2) + 1;
	}
	return minKeys;
}

//Delete the key specified and make adjustments to have a balanced b+ tree
Node * removeKey(BTMgr * BTMng, Node * node, Value * key, void * ptr)
{
	int maxChildrenNode = BTMng->mxchld;
	node = removeNodeKey(BTMng, node, key, ptr);
	if (node == BTMng->Root_node)
		return root_changes(BTMng->Root_node);
	int a = maxChildrenNode - 1;
	int minKeys = setMinKeys(node, a);
	if (node->nodeKeys >= minKeys)
		return BTMng->Root_node;

	int L_ind = getNeighbourInfo(node); //gets NeighbourInfo
	int ind;
	if(ind = L_ind == -1)
		ind = L_ind = 0;
	else
		ind = L_ind;

	Node * L_node;
	if(L_node == (L_ind == -1)){
		L_node = node->ptNode->ptrKy[1];
	}
	else{
		L_node= node->ptNode->ptrKy[L_ind];
	}
	//set space available
	int space = setSpaceAvailable(node, maxChildrenNode); //setSpaceAvailable
	return setValueForNode(node, L_node, L_ind, ind, space); //returns available space
}

//delete the record corresponding to the key
Node * removeByKey(BTMgr * BTMng, Value * key)
{
	Node * key_node = leafSearch(BTMng->Root_node, key);
	Node * rec = (Node *) keySearch(BTMng->Root_node, key);

	if (rec != NULL && key_node != NULL) {
		BTMng->Root_node = removeKey(BTMng, key_node, key, rec); //remove keys
		free(rec);
	}
	else
		return RC_ERROR;
	return BTMng->Root_node;
}
void leftIndNotFound(Node * node, Node * L_node, Node * t, int ind, Value * value){
	if (!node->isLeaf) {
		node->curKy[node->nodeKeys] = value;
		node->ptrKy[node->nodeKeys + 1] = L_node->ptrKy[0];
		t = (Node *) node->ptrKy[node->nodeKeys + 1];
		t->ptNode = node;
		node->ptNode->curKy[ind] = L_node->curKy[0];
	}
	else {
		node->curKy[node->nodeKeys] = L_node->curKy[0];
		node->ptrKy[node->nodeKeys] = L_node->ptrKy[0];
		node->ptNode->curKy[ind] = L_node->curKy[1];
	}
}

void elsePartKeyDistribution(Node * node, Node * L_node, Node * t, Value * value, int ind){
	if (!node->isLeaf) {
		node->ptrKy[0] = L_node->ptrKy[L_node->nodeKeys]; // node ptr key initializes
		t = (Node *) node->ptrKy[0];
		t->ptNode = node;
		L_node->ptrKy[L_node->nodeKeys] = NULL;
		node->curKy[0] = value;
		node->ptNode->curKy[ind] = L_node->curKy[L_node->nodeKeys - 1];
	}
	else {
		node->ptrKy[0] = L_node->ptrKy[L_node->nodeKeys - 1];
		L_node->ptrKy[L_node->nodeKeys - 1] = NULL;
		node->curKy[0] = L_node->curKy[L_node->nodeKeys - 1];
		node->ptNode->curKy[ind] = node->curKy[0];
	}
}
// Redistribution of keys after deletion
Node * keyRedistibution(Node * r_node, Node * node, Node * L_node, int L_ind, int ind, Value * value)
{
	Node * t;
	if (L_ind = -1) {
		leftIndNotFound(node, L_node, t, ind, value); // Left Node Not Found
		int i=0;
		while(i < L_node->nodeKeys - 1) {
			L_node->curKy[i] = L_node->curKy[i + 1];
			L_node->ptrKy[i] = L_node->ptrKy[i + 1];
			i =i+1;
		}
		if (!node->isLeaf)
			L_node->ptrKy[i] = L_node->ptrKy[i + 1];
	}
	else {
		if (!node->isLeaf)
			node->ptrKy[node->nodeKeys + 1] = node->ptrKy[node->nodeKeys];
		int i = node->nodeKeys;
		while(i > 0) {
			node->curKy[i] = node->curKy[i - 1];
			node->ptrKy[i] = node->ptrKy[i - 1];
			i = i-1;
		}
		elsePartKeyDistribution(node, L_node, t, value, ind);
	}
	node->nodeKeys = node->nodeKeys +1 ;
	L_node->nodeKeys = L_node->nodeKeys - 1;
	return r_node;
}



//Sequence for the printng of b+ tree
void seqInsert(BTMgr * BTMng, Node * node)
{
	//if Null, just add the new node and then NULL
	if (BTMng->prtseq == NULL) {
		BTMng->prtseq = node;
		BTMng->prtseq->next = NULL;
	} else {
		Node * slider;
		slider = BTMng->prtseq;
		while (slider->next != NULL)
			slider = slider->next;
		slider->next = node;
		node->next = NULL;
	}
}

//get the print seq for b+ tree
Node * getSeq(BTMgr * BTMng)
{
	Node * seq = BTMng->prtseq; // Initialize Scquence
	BTMng->prtseq = BTMng->prtseq->next;
	seq->next = NULL;
	return seq;
}

// Gives length from given node to the root.
int reachRoot(Node * r_node, Node * ch_node) {
	int s = 0;
	Node * c_node = ch_node; // Initialize NodeData
	while (c_node != r_node)
	{
		c_node = c_node->ptNode;
		s = s +1;
	}
	return s;
}

// less than, comparison for keys.
bool isLess(Value * key1, Value * key2) {
	switch (key1->dt) {
	case DT_INT:
		if (key1->v.intV < key2->v.intV) { //Logical Statement starts
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_FLOAT:
		if (key1->v.floatV < key2->v.floatV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_STRING:
		if (strcmp(key1->v.stringV, key2->v.stringV) == -1) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_BOOL:
		return FALSE;
		break;
	}
}

// returns if the two keys are equal or not
bool isEqual(Value * key1, Value * key2) {
	switch (key1->dt) {
	case DT_INT:
		if (key1->v.intV == key2->v.intV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_FLOAT:
		if (key1->v.floatV == key2->v.floatV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_STRING:
		if (strcmp(key1->v.stringV, key2->v.stringV) == 0) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_BOOL:
		if (key1->v.boolV == key2->v.boolV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	}
}

// Greater than, comparison for keys.
bool isGreater(Value * key1, Value * key2) {
	switch (key1->dt) {
	case DT_INT:
		if (key1->v.intV > key2->v.intV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_FLOAT:
		if (key1->v.floatV > key2->v.floatV) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_STRING:
		if (strcmp(key1->v.stringV, key2->v.stringV) == 1) {
			return TRUE;
		} else {
			return FALSE;
		}
		break;
	case DT_BOOL:
		return FALSE;
		break;
	}
}
