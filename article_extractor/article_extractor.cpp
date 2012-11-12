#include <Python.h>
#include "base.h"
#include "htmlclean.h"
#include <stdio.h>

static TStr readFile(TStr path) {
	FILE * pFile;
  unsigned long lSize;
  char * buffer;
  size_t result;

  pFile = fopen (path.CStr(), "rb");
  if (pFile==NULL) {fputs ("File error",stderr); exit (1);}

  // obtain file size:
  fseek (pFile , 0 , SEEK_END);
  lSize = ftell (pFile);
  rewind (pFile);

  // allocate memory to contain the whole file:
  buffer = (char*) malloc (sizeof(char)*lSize);
  if (buffer == NULL) {fputs ("Memory error",stderr); exit (2);}

  // copy the file into the buffer:
  result = fread (buffer,1,lSize,pFile);
  if (result != lSize) {fputs ("Reading error",stderr); exit (3);}

  /* the whole file is now loaded in the memory buffer. */
	TStr ret(buffer);

  // terminate
  fclose (pFile);
  free (buffer);

	return ret;
}

int main() {
	TStr html = readFile("sample.html");
	int a=-1, b=-1;
	THtmlClean *cleaner = new THtmlClean();
	TStr txt;
	txt = cleaner->Extract(html);
	delete cleaner;
	printf(">>%s<< %d %d\n", txt.CStr(), a, b);
}

TStr stripTags(TStr html) {
	char *ret = new char[html.Len()+1]; int retLen=0;
	char inQuotes=0;
	bool inTag=false;
	for (int i=0; i<html.Len(); i++) {
		char c = html[i];
		if (inTag) {
			if (c=='\'' || c=='"') {
				if (!inQuotes) inQuotes = c;
				else if (c==inQuotes) inQuotes = false;
			}
			if (inQuotes==0 && c=='>') { inTag = false; continue; }
		} else {
			if (c=='<') inTag = true;
		}

		if (!inTag) {
			ret[retLen++]=c;
		}
	}
	ret[retLen] = '\0';
	TStr retStr(ret);
	delete[] ret;
	return retStr;
}

extern "C" {

THtmlClean *cleaner;

static PyObject *
article_extractor_get_cleartext(PyObject *self, PyObject *args)
{
	const char *html_c;
	if (!PyArg_ParseTuple(args, "s", &html_c))
		return NULL;
	TStr html(html_c);
	/*
	html.ChangeStrAll("<p>","\n<p>");
	html.ChangeStrAll("<p ","\n<p ");
	html.ChangeStrAll("<br>","\n<br>");
	html.ChangeStrAll("<br/>","\n<br/>");
	html.ChangeStrAll("<br />","\n<br />");
	*/
	TStr txt = cleaner->Extract(html);
	//txt = stripTags(txt);
	if (txt.Len() > 100000) {
		// the cleaner probably got it wrong
		txt = "";
	}
	/*
	txt.ChangeStrAll("\r\n","\n");
	txt.ChangeStrAll("\r","\n");
	while (txt.ChangeStrAll("\n ","\n")>0);
	while (txt.ChangeStrAll("\n\n","\n")>0);
	*/
	return Py_BuildValue("s", txt.CStr());
}

static PyMethodDef PyMethods[] = {
    {"get_cleartext",  article_extractor_get_cleartext, METH_VARARGS,
     "Given HTML of a news article, return cleartext of the article body."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC
initarticle_extractor(void)
{
	cleaner = new THtmlClean();
	(void) Py_InitModule("article_extractor", PyMethods);
}

}
