/**
 * btree.cpp
 * This file includes method implementations of the BTreeIndex class (btree.h)
 *
 * @author Aly Valliani
 * @author Oscar Chen
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, 
 * University of Wisconsin-Madison.
 */

#include "btree.h"
#include "include/fileScanner.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/page_pinned_exception.h"

#include <iostream>
using namespace std;

using std::string;

namespace wiscdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufferManager *bufMgrIn,
		const int attrByteOffset){

  //Initializing data members
  scanExecuting = false;
  this->attrByteOffset = attrByteOffset;
  rootPageNum = Page::INVALID_NUMBER;
  bufferManager = bufMgrIn;
  
  std::stringstream ss;
  ss << relationName << '.' << attrByteOffset;
  outIndexName = ss.str();

  try {
    file = new RawFile(outIndexName, false);
    //Load an existing index
    headerPageNum = 1; //first page of every index file is the header
    IndexMetaInfo* header = getHeader();
    rootPageNum = header->rootPageNo;

    //verify info is correct
    if(strcmp(header->relationName, relationName.c_str())) {
      delete file;
      throw BadIndexInfoException("Relation name of existing index file did not match the inputted relation name");
    }
    if(header->attrByteOffset != attrByteOffset) {
      delete file;
      throw BadIndexInfoException("Attribute byte offset of existing index file did not match the inputted attribute byte offset");
    }
    //unpin page
    bufferManager->unPinPage(file, headerPageNum, false);
  } catch (FileNotFoundException e) {
    file = new RawFile(outIndexName, true);
    //Build a new index
    Page* headerPage;
    bufferManager->allocatePage(file, headerPageNum, headerPage); //allocates header page
    IndexMetaInfo* header = (IndexMetaInfo*) headerPage; //get index metadata
    strncpy(header->relationName, relationName.c_str(), 20); //sets header->relationName
    header->attrByteOffset = attrByteOffset; //sets header->attrByteOffset
    header->rootPageNo = rootPageNum; //sets header->rootPageNo
 
    FileScanner* fscan = new FileScanner(relationName, bufMgrIn);
    try
    {
      RecordId scanRid;
      while(1)
      {
        fscan->scanNext(scanRid);
        //Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
        std::string recordStr = fscan->getRecord();
        const char *record = recordStr.c_str();
        const char* key = record + attrByteOffset; //maybe we have to strcpy this?
        insertEntry(key, scanRid); //commented while testing constructor
      }
    } catch(EndOfFileException e){
      delete fscan;
    }
    //unpin page when done
    bufferManager->unPinPage(file, headerPageNum, true);
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex(){
  if(scanExecuting) {
    endScan();
  }
  bufferManager->flushFile(file);
  delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const char*key, const RecordId rid) {
  if(rootPageNum == Page::INVALID_NUMBER) { //Special case: first insert
    NonLeafNode *rootNode = allocateNonLeafNode(file, rootPageNum);
    rootNode->level = 1;
    getHeader()->rootPageNo = rootPageNum;
    bufferManager->unPinPage(file, headerPageNum, true);
    strncpy(rootNode->keyArray[0], key, STRINGSIZE);
    LeafNode *leaf1 = allocateLeafNode(file, rootNode->pageNoArray[0]);
    LeafNode *leaf2 = allocateLeafNode(file, rootNode->pageNoArray[1]);
    leaf1->rightSibPageNo = rootNode->pageNoArray[1];
    leaf2->rightSibPageNo = Page::INVALID_NUMBER;
    strncpy(leaf2->keyArray[0], key, STRINGSIZE);
    leaf2->ridArray[0] = rid;
    // unpin all pages in use
    bufferManager->unPinPage(file, rootPageNum, true);
    bufferManager->unPinPage(file, rootNode->pageNoArray[0], true);
    bufferManager->unPinPage(file, rootNode->pageNoArray[1], true);
    return;
  }
  // if insert into filled tree
  RIDKeyPair ridkey; PageKeyPair pagekey;
  ridkey.set(rid, key);
  insertInSubtree(ridkey, rootPageNum, pagekey);
}
      
// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const char* lowValParm,
				   const Operator lowOpParm,
				   const char* highValParm,
				   const Operator highOpParm){
  //Check if another scan is already executing
  if(scanExecuting) { endScan(); }
  //Check for bad input
  if(strncmp(lowValParm, highValParm, STRINGSIZE) > 0) {
    throw BadScanrangeException();
  }
  if(lowOpParm != GT && lowOpParm != GTE) {
    throw BadOpcodesException();
  }
  if(highOpParm != LT && highOpParm != LTE) {
    throw BadOpcodesException();
  }
  //Initialize scan data members
  scanExecuting = true;
  nextEntry = 0;
  lowVal = lowValParm;
  highVal = highValParm;
  lowOp = lowOpParm;
  highOp = highOpParm;
  findInSubtree(rootPageNum); // will set currentPageNum and currentPageData
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) {
  if(!scanExecuting) { throw ScanNotInitializedException(); }
  LeafNode *currNode = (LeafNode*) currentPageData;
  int nextPageNo, numKeys;
  // check if scan is at end
  if(currentPageNum != Page::INVALID_NUMBER && matchRange(currNode->keyArray[nextEntry])) {
    numKeys = getLeafLength(currNode);    
    outRid = currNode->ridArray[nextEntry];
  }
  else {
    endScan();
    throw IndexScanCompletedException();
  }
  // check if at the end of a leaf
  if(nextEntry == numKeys-1) {
    nextEntry = 0;
    nextPageNo = currNode->rightSibPageNo;
    bufferManager->unPinPage(file, currentPageNum, false);
    currentPageNum = nextPageNo;
    if(currentPageNum != Page::INVALID_NUMBER) {
      bufferManager->readPage(file, currentPageNum, currentPageData);
    }
    else { currentPageData = NULL; }
  }
  else { nextEntry++; }
  return;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------

const void BTreeIndex::endScan() {
  if(!scanExecuting) { throw ScanNotInitializedException(); }
  scanExecuting = false;
  if(currentPageNum != Page::INVALID_NUMBER) {
    bufferManager->unPinPage(file, currentPageNum, false);
    currentPageNum = Page::INVALID_NUMBER;
    currentPageData = NULL;
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::printTree
// -----------------------------------------------------------------------------

void BTreeIndex::printTree()
{
  printf("====BEGIN PRINT TREE====\n");
  if (rootPageNum == Page::INVALID_NUMBER) { printf("\t (empty tree)\n"); }
  else { printSubtree( rootPageNum ); }
  printf("====END PRINT TREE====\n");
}

/**********************PRIVATE HELPER METHODS*************************/

bool BTreeIndex::insertInSubtree(RIDKeyPair krid,
                                 PageId pageNum,
                                 PageKeyPair& splitKey){

  NonLeafNode *currNode = readNonLeafNode(file, pageNum);
  int currNodeLen = getNonLeafLength(currNode);
  bool split;
  if(strncmp(krid.key, currNode->keyArray[0], STRINGSIZE) < 0) { // insert in first page
    if (currNode->level == 1) {
      split = insertInLeaf(krid, currNode->pageNoArray[0], splitKey);
    }
    else {
      split = insertInSubtree(krid, currNode->pageNoArray[0], splitKey);
    }
  }
  else if (strncmp(krid.key, currNode->keyArray[currNodeLen-1], STRINGSIZE) >= 0) { // insert in last page
    if (currNode->level == 1) {
      split = insertInLeaf(krid, currNode->pageNoArray[currNodeLen], splitKey);
    }
    else {
      split = insertInSubtree(krid, currNode->pageNoArray[currNodeLen], splitKey);
    }
  }
  else { //insert in some middle page
    if (currNode->level == 1) {
      for(int i = 0; i < currNodeLen - 1; i++) {
        if((strncmp(currNode->keyArray[i], krid.key, STRINGSIZE) <= 0) && (strncmp(krid.key, currNode->keyArray[i+1], STRINGSIZE) < 0)) {
          split = insertInLeaf(krid, currNode->pageNoArray[i+1], splitKey); // i+1, right?
          break;
        }
      }
    }
    else {
      for(int i = 0; i < currNodeLen - 1; i++) {
        if((strncmp(currNode->keyArray[i], krid.key, STRINGSIZE) <= 0) && (strncmp(krid.key, currNode->keyArray[i+1], STRINGSIZE) < 0)) {
          split = insertInSubtree(krid, currNode->pageNoArray[i+1], splitKey); // i+1, right?
          break;
        }
      }
    }
  }
  if (split) { //if passed up splitkey 
    if(isRoomyNonLeaf(currNode)) { 
      insertInRoomyNonLeaf(currNode, splitKey);
      bufferManager->unPinPage(file, pageNum, true);
      return false;
    }
    else { // node is full
      PageId newPageNum;
      NonLeafNode* newNode = allocateNonLeafNode(file, newPageNum);
      newNode->level = currNode->level;
      //split node
      int i;
      int temp = currNode->pageNoArray[NON_LEAF_NUM_KEYS/2];
      for (i = NON_LEAF_NUM_KEYS/2; i < NON_LEAF_NUM_KEYS; i++) { // shift/delete
        strncpy(newNode->keyArray[i - NON_LEAF_NUM_KEYS/2], currNode->keyArray[i], STRINGSIZE);
        newNode->pageNoArray[i - NON_LEAF_NUM_KEYS/2] = temp;
        strncpy(currNode->keyArray[i], std::string(STRINGSIZE, '\0').c_str(), STRINGSIZE);
        temp = currNode->pageNoArray[i+1];
        currNode->pageNoArray[i+1] = Page::INVALID_NUMBER;
      }
      newNode->pageNoArray[i - NON_LEAF_NUM_KEYS/2] = temp;

      //get middle key
      char midKey[STRINGSIZE]; //middle key to push up
      if (strncmp(splitKey.key, newNode->keyArray[0], STRINGSIZE) < 0) { //key should go in old node
        insertInRoomyNonLeaf(currNode, splitKey);
        int currNodeLen = getNonLeafLength(currNode);
        strncpy(midKey, currNode->keyArray[currNodeLen-1], STRINGSIZE); // set midKey to last key in old node
        strncpy(currNode->keyArray[currNodeLen-1], std::string(STRINGSIZE, '\0').c_str(), STRINGSIZE); //delete last key
        newNode->pageNoArray[0] = currNode->pageNoArray[currNodeLen];
        currNode->pageNoArray[currNodeLen] = Page::INVALID_NUMBER;
      }
      else {  //key should go in new node
        insertInRoomyNonLeaf(newNode, splitKey);
        strncpy(midKey, newNode->keyArray[0], STRINGSIZE); //set midKey to first key in new node
        int newNodeLength = getNonLeafLength(newNode);
        for (int i = 0; i < newNodeLength; i++) { //shift
          strncpy(newNode->keyArray[i], newNode->keyArray[i+1], STRINGSIZE);
          newNode->pageNoArray[i] = newNode->pageNoArray[i+1];
        }
        strncpy(newNode->keyArray[newNodeLength-1], std::string(STRINGSIZE, '\0').c_str(), STRINGSIZE);
        newNode->pageNoArray[newNodeLength] = Page::INVALID_NUMBER; //reset pageNoArray
      }

      // check if root or internal node
      if (pageNum == rootPageNum) { //if current node is root, have to make new root
        NonLeafNode* newRoot = allocateNonLeafNode(file, rootPageNum);
        getHeader()->rootPageNo = rootPageNum;
        bufferManager->unPinPage(file, headerPageNum, true);
        strncpy(newRoot->keyArray[0], midKey, STRINGSIZE);
        newRoot->pageNoArray[0] = pageNum;
        newRoot->pageNoArray[1] = newPageNum;
        newRoot->level = currNode->level + 1;
        bufferManager->unPinPage(file, rootPageNum, true);
      }
      else { //if not, then pass up the middle key to the upper level insertInSubtree
        strncpy(splitKey.key, midKey, STRINGSIZE);
        splitKey.pageNo = newPageNum;
      }
      // unpin currNode and newNode
      bufferManager->unPinPage(file, pageNum, true);
      bufferManager->unPinPage(file, newPageNum, true);
      return true;
    }
  }
  else { //unpin the node
    bufferManager->unPinPage(file, pageNum, false);
    return false;
  }
  
}

bool BTreeIndex::insertInLeaf(RIDKeyPair krid,
                              PageId pageNum,
                              PageKeyPair& splitKey) {
  LeafNode* currLeaf = readLeafNode(file, pageNum);
  if (isRoomyLeaf(currLeaf)) {
    insertInRoomyLeaf(currLeaf, krid);
    bufferManager->unPinPage(file, pageNum, true);
    return false;
  }
  else { //leaf is full
    PageId newPageNum; 
    LeafNode* newLeaf = allocateLeafNode(file, newPageNum);
    for (int i = LEAF_NUM_KEYS/2; i < LEAF_NUM_KEYS; i++) { // evenly distributing keys
      strncpy(newLeaf->keyArray[i - LEAF_NUM_KEYS/2], currLeaf->keyArray[i], STRINGSIZE);
      newLeaf->ridArray[i - LEAF_NUM_KEYS/2] = currLeaf->ridArray[i];
      strncpy(currLeaf->keyArray[i], std::string(STRINGSIZE, '\0').c_str(), STRINGSIZE);
      currLeaf->ridArray[i].page_number = Page::INVALID_NUMBER;
    }
    if (strncmp(krid.key, newLeaf->keyArray[0], STRINGSIZE) < 0) { // insert into old leaf
      insertInRoomyLeaf(currLeaf, krid);
    }
    else { //insert into new leaf
      insertInRoomyLeaf(newLeaf, krid);
    }
    newLeaf->rightSibPageNo = currLeaf->rightSibPageNo; //set newLeaf's rightSibPageNo
    currLeaf->rightSibPageNo = newPageNum; //set currLeaf's rightSibPageNo to currLeaf's rightSibPage
    splitKey.pageNo = newPageNum;
    strncpy(splitKey.key, newLeaf->keyArray[0], STRINGSIZE); //copy up min key of new leaf
    bufferManager->unPinPage(file, pageNum, true);
    bufferManager->unPinPage(file, newPageNum, true);
    return true; //splitKey will get pushed up
  }
}

void BTreeIndex::findInSubtree(PageId currPid){
  NonLeafNode *currNode = readNonLeafNode(file, currPid);
  int numKeys = getNonLeafLength(currNode);  
  int i;
  for (i = 0; i < numKeys; i++) {
    if (lowOp == GT) {
      if (strncmp(currNode->keyArray[i], lowVal, STRINGSIZE) > 0) { break; }
    }
    else { //lowOp == GTE
      if (i == numKeys-1) { //last key
        if (strncmp(currNode->keyArray[i], lowVal, STRINGSIZE) > 0) { break; }
        //else case just finishes for loop
      }
      else {
        if (strncmp(currNode->keyArray[i], lowVal, STRINGSIZE) == 0) { i++; break; }
        else if (strncmp(currNode->keyArray[i], lowVal, STRINGSIZE) > 0) { break; }
      }
    }
  }
  //check if the node is right above leaves
  if (currNode->level != 1) {
    bufferManager->unPinPage(file, currPid, false);
    findInSubtree(currNode->pageNoArray[i]);
  }
  else { //is right above a leaf
    currentPageNum = currNode->pageNoArray[i];
    bufferManager->unPinPage(file, currPid, false);
    bufferManager->readPage(file, currentPageNum, currentPageData);

    RecordId rec;
    rec.page_number = Page::INVALID_NUMBER;
    rec.slot_number = Page::INVALID_SLOT;
    findInLeaf(currentPageNum, rec);
    if(rec.page_number == Page::INVALID_NUMBER) { //no record that matched param range found
      endScan();
      throw NoSuchKeyFoundException();
    }
  }
  return;
}

void BTreeIndex::findInLeaf(PageId currPid, RecordId& result) {
  LeafNode *currNode = (LeafNode*) currentPageData;
  int numKeys = getLeafLength(currNode);
  for(int i = 0; i < numKeys; i++) { //iterate through keys in the leaf
    if(matchRange(currNode->keyArray[i])) {
      result = currNode->ridArray[i];
      nextEntry = i;
      return;
    }
    else if(strncmp(currNode->keyArray[i], highVal, STRINGSIZE) > 0) { //if current key is larger highVal, return
      return;
    }
  }
  if(currNode->rightSibPageNo != Page::INVALID_NUMBER) { //jump to right sibling node if rightSibPageNo is valid
    currentPageNum = currNode->rightSibPageNo;
    bufferManager->unPinPage(file, currPid, false);
    bufferManager->readPage(file, currentPageNum, currentPageData);
    findInLeaf(currentPageNum, result);
  }
  return;
}

void BTreeIndex::printSubtree(PageId pageNum){
  Page* nodePage;
  int numKeys;
  //Load node into memory
  bufferManager->readPage(file, pageNum, nodePage);
  NonLeafNode* node = (NonLeafNode*) nodePage;
  numKeys = getNonLeafLength(node);
  //Print out the Level and PageId for reference
  printf("***NON-LEAF***\tLevel: %d, pageId: %d, length: %d\n", node->level, pageNum, numKeys);
  //Print out each key/page pair
  for (int i = 0; i < numKeys; i++) {
    printf(" {%d} | (%.10s) | ", node->pageNoArray[i], node->keyArray[i]);
  }
  printf("{%d}\n", node->pageNoArray[numKeys]); //extra pageId
  //For each child node, call recursively
  for (int i = 0; i <= numKeys; i++) {
    if (node->level == 1) {
      printLeaf(node->pageNoArray[i]); // child is a Leaf
    }
    else {
      printSubtree(node->pageNoArray[i]); // child is a nonLeaf
    }
  }
  // unpin the page
  bufferManager->unPinPage(file, pageNum, false);
}

void BTreeIndex::printLeaf(PageId pageNum){
  Page* nodePage;
  int numKeys;
  //Load node into memory
  bufferManager->readPage(file, pageNum, nodePage);
  LeafNode* node = (LeafNode*) nodePage;
  numKeys = getLeafLength(node);
  //Print out the level and pageId for reference
  printf("\t***LEAF***\tpageId: %d, rightSibPageNo: %d, length: %d\n", pageNum, node->rightSibPageNo, numKeys);
  //Print out each key/rid pair
  if (!numKeys) {
    printf("\t(empty)\n");
  }
  else {
    printf("\t");
    for (int i = 0; i < numKeys; i++) {
      printf("(%.10s, [%d, %d]) | ", node->keyArray[i], node->ridArray[i].page_number, node->ridArray[i].slot_number);
    }
  printf("\n");
  }
  //unpin the page
  bufferManager->unPinPage(file, pageNum, false);
}

NonLeafNode* BTreeIndex::allocateNonLeafNode(File *fptr, PageId &pageNo) { 
  Page* page;
  bufferManager->allocatePage(fptr, pageNo, page);
  return (NonLeafNode*) page;
}


LeafNode* BTreeIndex::allocateLeafNode(File *fptr, PageId& pageNo) {
  Page* page;
  bufferManager->allocatePage(fptr, pageNo, page);
  return (LeafNode*) page;
}

NonLeafNode* BTreeIndex::readNonLeafNode(File *fptr, PageId &pageNo) {
  Page* page;
  bufferManager->readPage(fptr, pageNo, page);
  return (NonLeafNode*) page;
}

LeafNode* BTreeIndex::readLeafNode(File *fptr, PageId &pageNo) {
  Page* page;
  bufferManager->readPage(fptr, pageNo, page);
  return (LeafNode*) page;
}

bool BTreeIndex::isRoomyLeaf(LeafNode* leaf) {
  return leaf->ridArray[LEAF_NUM_KEYS-1].page_number == Page::INVALID_NUMBER;
}

bool BTreeIndex::isRoomyNonLeaf(NonLeafNode* node) {
  return node->pageNoArray[LEAF_NUM_KEYS] == Page::INVALID_NUMBER;
}

void BTreeIndex::insertInRoomyLeaf(LeafNode* leaf, RIDKeyPair krid) {
  for (int i = 0; i < LEAF_NUM_KEYS; i++) {
    if(leaf->ridArray[i].page_number == Page::INVALID_NUMBER) { 
      strncpy(leaf->keyArray[i], krid.key, STRINGSIZE);
      leaf->ridArray[i] = krid.rid;
      return;
    }
    if (strncmp(leaf->keyArray[i], krid.key, STRINGSIZE) >= 0) { //if key in array is greater than key to insert
      for (int j = LEAF_NUM_KEYS - 2; j >= i; j--) { //shift everything down
        strncpy(leaf->keyArray[j+1], leaf->keyArray[j], STRINGSIZE);
        leaf->ridArray[j+1] = leaf->ridArray[j];
      }
      strncpy(leaf->keyArray[i], krid.key, STRINGSIZE);
      leaf->ridArray[i] = krid.rid;
      return;
    }
  }
}

void BTreeIndex::insertInRoomyNonLeaf(NonLeafNode* node, PageKeyPair pageKey) {
  for(int i = 0; i < NON_LEAF_NUM_KEYS; i++) {
    if(node->pageNoArray[i+1] == Page::INVALID_NUMBER) {
      strncpy(node->keyArray[i], pageKey.key, STRINGSIZE);
      node->pageNoArray[i+1] = pageKey.pageNo;
      return;
    }
    if (strncmp(node->keyArray[i], pageKey.key, STRINGSIZE) >= 0) { //if key in array is greater than key to insert
      for (int j = NON_LEAF_NUM_KEYS - 2; j >= i; j--) { //shift everything down
        strncpy(node->keyArray[j+1], node->keyArray[j], STRINGSIZE);
        node->pageNoArray[j+2] = node->pageNoArray[j+1];
      }
      strncpy(node->keyArray[i], pageKey.key, STRINGSIZE);
      node->pageNoArray[i+1] = pageKey.pageNo;
      return;
    }
  }
}

int BTreeIndex::getNonLeafLength(NonLeafNode* node) {
  for (int i = 1; i < NON_LEAF_NUM_KEYS+1; i++) {
    if (node->pageNoArray[i] == Page::INVALID_NUMBER) {
      return i-1;
    }
  }
  return NON_LEAF_NUM_KEYS;
}

int BTreeIndex::getLeafLength(LeafNode* node) {
  for (int i = 0; i < LEAF_NUM_KEYS; i++) {
    if (node->ridArray[i].page_number == Page::INVALID_NUMBER) {
      return i;
    }
  }
  return LEAF_NUM_KEYS;
}

bool BTreeIndex::matchRange(const char* key) {
  bool lowFit, highFit;
  if(lowOp == GT) {
    lowFit = (strncmp(key, lowVal, STRINGSIZE) > 0);
  }
  else {
    lowFit = (strncmp(key, lowVal, STRINGSIZE) >= 0);
  }
  if(highOp == LT) {
    highFit = (strncmp(key, highVal, STRINGSIZE) < 0);
  }
  else {
    highFit = (strncmp(key, highVal, STRINGSIZE) <= 0);
  }
  return (lowFit && highFit);
}

IndexMetaInfo* BTreeIndex::getHeader() {
  Page* headerPage;
  bufferManager->readPage(file, headerPageNum, headerPage);
  return (IndexMetaInfo*) headerPage;
}

}
