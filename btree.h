/**
 * @author Aly Valliani
 * @author Oscar Chen
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>

#include "include/types.h"
#include "include/page.h"
#include "include/file.h"
#include "include/buffer.h"

//Uncomment next line to reduce size of nodes and make prints more readable
// DEBUG mode uses just 8 keys in a node making splits more frequent
#define DEBUG

namespace wiscdb
{


/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{ 
  LT,   /* Less Than */
  LTE,  /* Less Than or Equal to */
  GTE,  /* Greater Than or Equal to */
  GT    /* Greater Than */
};

/**
 * @brief Size of String key prefix.
 */
const  int STRINGSIZE = 10;


/**
 * @brief Number of keys stored in B+Tree leaf / non-leaf for prefix strings.
 */
#ifdef DEBUG
const int LEAF_NUM_KEYS = 4;
const int NON_LEAF_NUM_KEYS = 4;
#else
const  int LEAF_NUM_KEYS =  
  (Page::SIZE - sizeof(PageId)) / (STRINGSIZE + sizeof(RecordId));
// free bytes - sibling ptr     /    size of one key,rid pair

const  int NON_LEAF_NUM_KEYS = 
  (Page::SIZE - sizeof(int) - sizeof(PageId)) / (STRINGSIZE + sizeof(PageId));
// free bytes - level - extra ptr         /   size of one key, pageid pair  
#endif



/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair
 * to functions that add to or make changes to the leaf node pages of the tree
 */
class RIDKeyPair{
 public:
  RecordId rid;
  char key[STRINGSIZE]; 
  void set( RecordId r, const char* k){
    rid = r;
    strncpy(key,k,STRINGSIZE);
  }
};

/**
 * @brief Structure to store a key-page pair which is used to pass the key 
 * and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
class PageKeyPair{
 public:
  PageId pageNo;
  char key[STRINGSIZE];
  void set( int p, const char* k){
    pageNo = p;
    strncpy(key,k,STRINGSIZE);
  }
};

/**
 * @brief The meta page, which holds metadata for Index file, is always
 * first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and
 * the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new pageId.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
  char relationName[20];

  /**
   * Offset of attribute for which we are indexing (i.e., where
   *   each key is located within on tuple)
   */
  int attrByteOffset;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
  PageId rootPageNo;
};

/*****
Each node is one page; a page is the main abstraction of our system.  When
requested, it is 8KB of "raw" data - there is no formatting.
We can map our own structure by casting the raw page as one of the structs
below.  That is, if we want the page to behave like an internal node, we
cast the page as a NonLeafNode.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of STRING type.
*/
struct NonLeafNode{
  /**
   * Level of the node in the tree.
   */
  int level;

  /**
   * Stores keys.
   */
  char keyArray[ NON_LEAF_NUM_KEYS ][ STRINGSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other 
   *   non-leaf/leaf nodes in the tree.
   */
  PageId pageNoArray[ NON_LEAF_NUM_KEYS + 1 ];
};

/**
 * @brief Structure for all leaf nodes when the key is of STRING type.
*/
struct LeafNode{
  /**
   * Stores keys.
   */
  char keyArray[ LEAF_NUM_KEYS ][ STRINGSIZE ];

  /**
   * Stores RecordIds to retrieve actual tuple from the associated heap file.
   */
  RecordId ridArray[ LEAF_NUM_KEYS ];

  /**
   * Page number of the leaf on the right side.
   * This linking of leaves allows to easily move from one leaf to the 
   * next leaf during index scan.
   */
  PageId rightSibPageNo;
};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single 
 * attribute of a relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
  File      *file;

  /**
   * Buffer Manager Instance.
   */
  BufferManager *bufferManager;

  /**
   * Page number of meta page.
   */
  PageId    headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
  PageId    rootPageNum;

   /**
   * Offset of attribute, over which index is built, inside records. 
   */
  int       attrByteOffset;

  // ********** MEMBERS SPECIFIC TO SCANNING ************ //

  /**
   * True if an index scan has been started.
   */
  bool      scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */ 
  int       nextEntry;

  /**
   * Page number of current page being scanned.
   */
  PageId    currentPageNum;

  /**
   * Current Page being scanned.
   */
  Page      *currentPageData;

  /**
   * Low STRING value for scan.
   */
  const char* lowVal;

  /**
   * High STRING value for scan.
   */
  const char* highVal;
  
  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
  Operator  lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
  Operator  highOp;

  
 public:

  /**
   * BTreeIndex Constructor. 
   * Check to see if the corresponding index file exists. If so, open the file.
   * If not, create it and insert entries for every tuple in the base 
   * relation using FileScanner class.
   *
   * @param relationName        Name of file.
   * @param outIndexName        Return the name of index file.
   * @param bufMgrIn            Buffer Manager Instance
   * @param attrByteOffset      Offset of attribute, over which index is 
   * to be built, in the record
   * @throws  BadIndexInfoException     If the index file already exists 
   * for the corresponding attribute, but values in metapage(relationName,
   * attribute byte offset, attribute type etc.) do not match with values 
   * received through constructor parameters.
   */
  BTreeIndex(const std::string & relationName, std::string & outIndexName,
            BufferManager *bufMgrIn,  const int attrByteOffset);
  

  /**
   * BTreeIndex Destructor. 
   * End any initialized scan, flush index file, after unpinning any 
   * pinned pages, from the buffer manager and delete file instance 
   * thereby closing the index file.
   * Destructor should not throw any exceptions. 
   * */
  ~BTreeIndex();


  /**
   * Insert a new entry using the pair <key,rid>. 
   * Start from root to recursively find out the leaf to insert the entry in.
   * The insertion may cause splitting of leaf node.
   * This splitting will require addition of new leaf page number entry 
   * into the parent non-leaf, which may in-turn get split.
   * This may continue all the way upto the root causing the root to get
   * split. If root gets split, metapage needs to be changed accordingly.
   * Make sure to unpin pages as soon as you can.
   * @param key      Key to insert, char string
   * @param rid      Record ID of a record whose entry is getting
   * inserted into the index.
  **/
  const void insertEntry(const char* key, const RecordId rid);


  /**
   * Begin a filtered scan of the index.  For instance, if the method is called
   * using ("a",GT,"d",LTE) then we should seek all entries with a value 
   * greater than "a" and less than or equal to "d".
   * If another scan is already executing, that needs to be ended here.
   * Set up all the variables for scan. Start from root to find out the
   * leaf page that contains the first RecordId
   * that satisfies the scan parameters. Keep that page pinned in the buffer
   * pool.
   * @param lowVal  Low value of range, pointer to  char string
   * @param lowOp    Low operator (GT/GTE)
   * @param highVal  High value of range, pointer char string
   * @param highOp  High operator (LT/LTE)
   * @throws  BadOpcodesException If lowOp and highOp do not contain one
   *   of their their expected values 
   * @throws  BadScanrangeException If lowVal > highval
   * @throws  NoSuchKeyFoundException If there is no key in the B+ tree 
   *   that satisfies the scan criteria.
  **/
  const void startScan(const char* lowVal, const Operator lowOp, const char* highVal, const Operator highOp);


  /**
   * Fetch the record id of the next index entry that matches the scan.
   * Return the next record from current page being scanned. If current page
   * has been scanned to its entirety, move on to the right sibling of 
   * current page, if any exists, to start scanning that page. Make sure to
   * unpin any pages that are no longer required.
   * @param outRid  RecordId of next record found that satisfies the scan
   *   criteria returned in this
   * @throws ScanNotInitializedException If no scan has been initialized.
   * @throws IndexScanCompletedException If no more records, satisfying the
   * scan criteria, are left to be scanned.  
  **/
  const void scanNext(RecordId& outRid);  // returned record id


  /**
   * Terminate the current scan. Unpin any pinned pages. Reset scan 
   *    specific variables.
   * @throws ScanNotInitializedException if no scan has been initialized.
  **/
  const void endScan();

  /**
   * Optional method for debugging: prints all keys in tree
   */
  void printTree(); 
  
 private:
  //You are not obligated to use these methods; feel free to delete,
  //  modify, or add to any of the methods below.  You *should not* modify
  //  the public interface above

  /**
   * Recursive helper method for insert; assumes the given page is an
   *   internal node
   * @param krid string prefix key and RecordId to insert in tree
   * @param pagenum PageId of the node to be searched; use pagenum
   *   to obtain the Page from the buffer manager
   * @param splitKey a reference parameter.  This contains no input value
   *   but could be used in case of a split to return the key/PageId pair
   *   to insert in the parent node
   * @return returns true if a split occurred
   */
  bool insertInSubtree(RIDKeyPair krid, PageId pageNum, PageKeyPair& splitKey);

  /**
   * Recursive helper method for inserting in the base case of reaching a leaf
   * @param krid string prefix key and RecordId to insert in tree
   * @param pagenum PageId of the leaf to be searched; use pagenum
   *   to obtain the Page from the buffer manager
   * @param splitKey a reference parameter.  This contains no input value
   *   but could be used in case of a split to return the key/PageId pair
   *   to insert in the parent node
   * @return returns true if a split occurred
   */
  bool insertInLeaf(RIDKeyPair krid, PageId pageNum, PageKeyPair& splitKey);

  /**
   * Recursive helper method for searching an internal node of the tree
   * @param currPid PageId of non-leaf node page
   */
  void findInSubtree(PageId currPid); 

    /**
   * Recursive helper method for searching in a leaf node of the tree
   * @param currPid PageId of non-leaf node page
   */
  void findInLeaf(PageId currPid, RecordId& result); 
    
  /**
   * Recursive helper method for printing out contents of tree
   * @param pageNum is the PageId of the node to read
   */
  void printSubtree(PageId pageNum);

  /**
   * Recursive helper method to print out leaf node
   */
  void printLeaf(PageId pageNum);

  /** 
   * Helper for allocating a page and then casting it to a NonLeafNode
   * @param fptr File of the non-leaf node page to allocate a page into
   * @param pageNo a reference parameter. Contains no input value, 
   *    returns the page number of the allocate non-leaf node page.
   * @return returns a NonLeafNode cast pointer to the allocated page
   */
  NonLeafNode* allocateNonLeafNode(File *fptr, PageId &pageNo);

  /**
   * Helper for allocating a page and then casting it to a LeafNode
   * @param fptr File of the leaf node page to allocate a page into
   * @param pageNo a reference parameter. Contains no input value, 
   *    returns the page number of the allocate leaf node page.
   * @return returns a LeafNode cast pointer to the allocated page
   */
  LeafNode* allocateLeafNode(File *fptr, PageId &pageNo);

  /**
   * Helper for reading a page and then casting it to a NonLeafNode
   * @param fptr File of the leaf node page to read the page from
   * @param pageNo a reference parameter. Page number of the non-leaf 
   *    node page being read
   * @return returns a NonLeafNode cast pointer to the read page
   */
  NonLeafNode* readNonLeafNode(File *fptr, PageId &pageNo);

  /**
   * Helper for reading a page and then casting it to a LeafNode
   * @param fptr File of the leaf node page to read the page from
   * @param pageNo a reference parameter. Page number of the leaf 
   *    node page being read
   * @return returns a LeafNode cast pointer to the read page
   */
  LeafNode* readLeafNode(File *fptr, PageId &pageNo);

  /**
   * Helper for determining whether leaf is at capacity or not
   * @param leaf Pointer to leaf whose capactiy is being checked
   * @return returns true if the leaf still has room for more entries
   */
  bool isRoomyLeaf(LeafNode* leaf);

  /**
   * Helper for determining whether a non-leaf is at capactiy or not
   * @param leaf Pointer to leaf whose capacity is being checked
   * @return returns true if the non-leaf still has room for more keys
   */
  bool isRoomyNonLeaf(NonLeafNode* node);

  /**
   * Inserts a key, record id pair into a leaf
   * @param leaf Pointer to leaf being inserted into
   * @param krid Pair of key, record id for index storing to insert into leaf
   */
  void insertInRoomyLeaf(LeafNode* leaf, RIDKeyPair krid);

  /**
   * Inserts a key, page id pair into a non-leaf
   * @param leaf Pointer to leaf being inserted into
   * @param krid Pair of key, page id for index storing to insert into leaf
   */
  void insertInRoomyNonLeaf(NonLeafNode* node, PageKeyPair pageKey);

  /**
   * Obtains the number of keys in a non-leaf node
   * @param node NonLeafNode pointer to check length of
   * @return returns the number of keys in the non-leaf node
   */
  int getNonLeafLength(NonLeafNode* node);

  /**
   * Obtains the number of keys in a leaf node
   * @param node LeafNode pointer to check length of
   * @return returns the number of keys in the leaf node
   */
  int getLeafLength(LeafNode* node);

  /**
   * Checks whether the key is within the search range provided by the user
   * @param key pointer to char string that is the key to be checked
   * @return true or false depending on whether the provided key is within
   * the scan range
   */
  bool matchRange(const char* key);

  /**
   * Obtains the header page for the specified index. A higher-level function
   * has to unpin the header page after it is done using it
   * @return returns an IndexMetaInfo* cast pointer to the index's header page
   */
  IndexMetaInfo* getHeader();
  
};

}
