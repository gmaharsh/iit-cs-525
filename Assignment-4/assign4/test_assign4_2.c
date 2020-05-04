#include <stdlib.h>

#include "dberror.h"
#include "expr.h"
#include "btree_mgr.h"
#include "tables.h"
#include "test_helper.h"

#define ASSERT_EQUALS_RID(_l,_r, message)				\
  do {									\
    ASSERT_TRUE((_l).page == (_r).page && (_l).slot == (_r).slot, message); \
  } while(0)

// test methods
static void float_test_insertFind (void);
static void string_test_insertFind (void);
static void float_test_del (void);

// helper methods
static Value **createVals (char **stringVals, int size);
static void freeVals (Value **vals, int size);
static int *createPermutation (int size);

// test name
char *testName;

// main method
int 
main (void) 
{
  testName = "";

  float_test_insertFind();
  float_test_del();
  string_test_insertFind();

  return 0;
}

void
float_test_insertFind (void)
{
  RID ins_values[] = { 
    {1,1},
    {2,3},
    {1,2},
    {3,5},
    {4,4},
    {3,2}, 
  };
  int n_inserts = 6;
  Value **curr_keys;
  char *str_keys[] = {
    "f2.0",
    "f1.5",
    "f0.1",
    "f4.5",
    "f10.5",
    "f20.4"
  };
  testName = "test b-tree inserting and search float";
  int i, testint;
  BTreeHandle *tree = NULL;
  
  curr_keys = createVals(str_keys, n_inserts);


  TEST_CHECK(initIndexManager(NULL));
  TEST_CHECK(createBtree("testidx", DT_FLOAT, 2));
  TEST_CHECK(openBtree(&tree, "testidx"));

  
  for(i = 0; i < n_inserts; i++)
    TEST_CHECK(insertKey(tree, curr_keys[i], ins_values[i]));

  // index stats check 
  TEST_CHECK(getNumNodes(tree, &testint));
  ASSERT_EQUALS_INT(testint,4, "number of nodes in btree");
  TEST_CHECK(getNumEntries(tree, &testint));
  ASSERT_EQUALS_INT(testint, n_inserts, "number of entries in btree");

  // keys search
  for(i = 0; i < 500; i++)
    {
      int pos = rand() % n_inserts;
      RID rid;
      Value *key = curr_keys[pos];

      TEST_CHECK(findKey(tree, key, &rid));
      ASSERT_EQUALS_RID(ins_values[pos], rid, "did we find the correct RID?");
    }

  TEST_CHECK(closeBtree(tree));
  TEST_CHECK(deleteBtree("testidx"));
  TEST_CHECK(shutdownIndexManager());
  freeVals(curr_keys, n_inserts);

  TEST_DONE();
}


void
float_test_del (void)
{
  RID ins_values[] = {
    {1,1},
    {2,3},
    {1,2},
    {3,5},
    {4,4},
    {3,2},
  };
  int n_inserts = 6;
  Value **curr_keys;
  char *str_keys[] = {
      "f2.0",
      "f1.5",
      "f0.1",
      "f4.5",
      "f10.5",
      "f20.4"
    };
  testName = "test b-tree inserting and search";
  int i, iter;
  BTreeHandle *tree = NULL;
  int n_dels = 3;
  bool *dels = (bool *) malloc(n_inserts * sizeof(bool));

  curr_keys = createVals(str_keys, n_inserts);


  TEST_CHECK(initIndexManager(NULL));

  // Random removal of entries and creation of b-tree
  for(iter = 0; iter < 50; iter++)
    {
      // random selection of entries
      for(i = 0; i < n_inserts; i++)
	dels[i] = FALSE;
      for(i = 0; i < n_dels; i++)
	dels[rand() % n_inserts] = TRUE;

      TEST_CHECK(createBtree("testidx", DT_FLOAT, 2));
      TEST_CHECK(openBtree(&tree, "testidx"));


      for(i = 0; i < n_inserts; i++)
	TEST_CHECK(insertKey(tree, curr_keys[i], ins_values[i]));

      // deletion of entries
      for(i = 0; i < n_inserts; i++)
	{
	  if (dels[i])
	    TEST_CHECK(deleteKey(tree, curr_keys[i]));
	}

      for(i = 0; i < 500; i++)
	{
	  int pos = rand() % n_inserts;
	  RID rid;
	  Value *key = curr_keys[pos];

	  if (dels[pos])
	    {
	      int rc = findKey(tree, key, &rid);
	      ASSERT_TRUE((rc == RC_IM_KEY_NOT_FOUND), "entry was deleted, should not find it");
	    }
	  else
	    {
	      TEST_CHECK(findKey(tree, key, &rid));
	      ASSERT_EQUALS_RID(ins_values[pos], rid, "did we find the correct RID?");
	    }
	}

      TEST_CHECK(closeBtree(tree));
      TEST_CHECK(deleteBtree("testidx"));
    }

  TEST_CHECK(shutdownIndexManager());
  freeVals(curr_keys, n_inserts);
  free(dels);

  TEST_DONE();
}


void
string_test_insertFind (void)
{
RID ins_values[] = {
	{1,1},
	{2,3},
	{1,2},
	{3,5},
	{4,4},
	{3,2},
  };
  int n_inserts = 6;
  Value **curr_keys;
  char *str_keys[] = {
  	"sxyz",
  	"sabc",
  	"scs525",
  	"siit",
  	"sxee",
  	"sbird"
  };
  testName = "test b-tree inserting and search string";
  int i, testint;
  BTreeHandle *tree = NULL;

  curr_keys = createVals(str_keys, n_inserts);


  TEST_CHECK(initIndexManager(NULL));
  TEST_CHECK(createBtree("testidx", DT_STRING, 2));
  TEST_CHECK(openBtree(&tree, "testidx"));

  for(i = 0; i < n_inserts; i++)
	TEST_CHECK(insertKey(tree, curr_keys[i], ins_values[i]));

  // index stats check
  TEST_CHECK(getNumNodes(tree, &testint));
  ASSERT_EQUALS_INT(testint, 3, "number of nodes in btree");
  TEST_CHECK(getNumEntries(tree, &testint));
  ASSERT_EQUALS_INT(testint, n_inserts, "number of entries in btree");


  for(i = 0; i < 500; i++)
	{
	  int pos = rand() % n_inserts;
	  RID rid;
	  Value *key = curr_keys[pos];
	  printf("\nFinding Key ==> %s ", key->v.stringV);
	  TEST_CHECK(findKey(tree, key, &rid));
	  ASSERT_EQUALS_RID(ins_values[pos], rid, "did we find the correct RID?");
	}

  TEST_CHECK(closeBtree(tree));
  TEST_CHECK(deleteBtree("testidx"));
  TEST_CHECK(shutdownIndexManager());
  freeVals(curr_keys, n_inserts);


  TEST_DONE();
}

int *
createPermutation (int size)
{
  int *res = (int *) malloc(size * sizeof(int));
  int i;

  for(i = 0; i < size; res[i] = i, i++);

  for(i = 0; i < 100; i++)
    {
      int l, r, t;
      l = rand() % size;
      r = rand() % size;
      t = res[l];
      res[l] = res[r];
      res[r] = t;
    }
  
  return res;
}

Value **
createVals (char **stringVals, int size)
{
  Value **res = (Value **) malloc(sizeof(Value *) * size);
  int i;
  
  for(i = 0; i < size; i++)
    res[i] = stringToValue(stringVals[i]);

  return res;
}

void
freeVals (Value **vals, int size)
{
  while(--size >= 0)
    free(vals[size]);
  free(vals);
}
