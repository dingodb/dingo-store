%module dingosdk 

%{
#include "sdk/status.h"
#include "common/slice.h"
#include "sdk/vector.h"
#include "sdk/client.h"

using namespace dingodb;
using namespace dingodb::sdk;
%}

%include <stdint.i>
%include <std_string.i>
%include <std_vector.i>
%include <std_map.i>
%include <std_unique_ptr.i>
%include <std_shared_ptr.i>
%include <typemaps.i>

%naturalvar;
%feature("autodoc", "3");

%ignore operator[];
%ignore operator=;

%template(Int64Vector)  std::vector<int64_t>;
%template(FloatVector) std::vector<float>;
%template(StringVector) std::vector<std::string>;

%template(KVPairVector) std::vector<dingodb::sdk::KVPair>;
%template(KeyOpStateVector) std::vector<dingodb::sdk::KeyOpState>;

%template(VectorWithIdVector) std::vector<dingodb::sdk::VectorWithId>;
%template(SearchResultVector) std::vector<dingodb::sdk::SearchResult>;
%template(DeleteResultVector) std::vector<dingodb::sdk::DeleteResult>;
%template(VectorWithDistanceVector) std::vector<dingodb::sdk::VectorWithDistance>;

%template(SearchExtraParamMap)  std::map<dingodb::sdk::SearchExtraParamType, int32_t>;

// client
// vector index creator
%apply bool& OUTPUT { bool& out_create_in_progress };
%apply int64_t& OUTPUT  { int64_t& out_index_id };
%apply int64_t& OUTPUT  { int64_t& out_vector_id };
%apply int64_t& OUTPUT  { int64_t& out_count };

// rawkv
%apply std::string& OUTPUT { std::string& out_value };
%apply bool& OUTPUT { bool& out_state };
%apply int64_t& OUTPUT { int64_t& out_delete_count };

// region creator
%apply int64_t& OUTPUT  { int64_t& out_region_id };

namespace dingodb {
    namespace sdk {
    
        %feature("docstring") Client::Build "return Status, Client"
        %feature("docstring") Client::NewRawKV "return Status, RawKV"
        %feature("docstring") Client::NewTransaction "return Status, Transaction"
        %feature("docstring") Client::NewRegionCreator "return Status, RegionCreator"
        %feature("docstring") Client::NewVectorClient "return Status, VectorClient"
        %feature("docstring") Client::NewVectorIndexCreator "return Status, VectorIndexCreator"
        %feature("docstring") Client::IsCreateRegionInProgress "return Status, bool out_create_in_progress"
        %feature("docstring") Client::GetIndexId "return Status, int64_t out_index_id"

        %feature("docstring") RawKV::Get "return Status, std::string out_value"
        %feature("docstring") RawKV::PutIfAbsent "return Status, bool out_state"
        %feature("docstring") RawKV::DeleteRangeNonContinuous "return Status, int64_t out_delete_count"
        %feature("docstring") RawKV::DeleteRange "return Status, int64_t out_delete_count"
        %feature("docstring") RawKV::CompareAndSet "return Status, bool out_state"

        %feature("docstring") RegionCreator::Create "return Status, int64_t out_region_id"

        %feature("docstring") VectorIndexCreator::Create "return Status, int64_t out_index_id"

        %feature("docstring") VectorClient::SearchByIndexId "return Status, std::vector<SearchResult> out_result"
        %feature("docstring") VectorClient::SearchByIndexName "return Status, std::vector<SearchResult> out_result"
        %feature("docstring") VectorClient::DeleteByIndexId "return Status, std::vector<DeleteResult> out_result"
        %feature("docstring") VectorClient::DeleteByIndexName "return Status, std::vector<DeleteResult> out_result"
        %feature("docstring") VectorClient::BatchQueryByIndexId "return Status, QueryResult out_result"
        %feature("docstring") VectorClient::BatchQueryByIndexName "return Status, QueryResult out_result"
        %feature("docstring") VectorClient::GetBorderByIndexId "return Status, int64_t out_vector_id"
        %feature("docstring") VectorClient::GetBorderByIndexName "return Status, int64_t out_vector_id"
        %feature("docstring") VectorClient::ScanQueryByIndexId "return Status, ScanQueryResult out_result"
        %feature("docstring") VectorClient::ScanQueryByIndexName "return Status, ScanQueryResult out_result"
        %feature("docstring") VectorClient::GetIndexMetricsByIndexId "return Status, IndexMetricsResult out_result"
        %feature("docstring") VectorClient::GetIndexMetricsByIndexName "return Status, IndexMetricsResult out_result"
        %feature("docstring") VectorClient::CountByIndexId "return Status, int64_t out_count"
        %feature("docstring") VectorClient::CountByIndexName "return Status, int64_t out_count"

        %typemap(in, numinputs=0) Client** (Client* temp){
          temp = NULL;
          $1 = &temp;
        }
        %typemap(argout) Client** {
          %append_output(SWIG_NewPointerObj(%as_voidptr(*$1), $*1_descriptor, SWIG_POINTER_OWN));
        }

        %typemap(in, numinputs=0) RawKV** (RawKV* temp){
          temp = NULL;
          $1 = &temp;
        }
        %typemap(argout) RawKV** {
          %append_output(SWIG_NewPointerObj(%as_voidptr(*$1), $*1_descriptor, SWIG_POINTER_OWN));
        }


        %typemap(in, numinputs=0) Transaction** (Transaction* temp){
          temp = NULL;
          $1 = &temp;
        }
        %typemap(argout) Transaction** {
          %append_output(SWIG_NewPointerObj(%as_voidptr(*$1), $*1_descriptor, SWIG_POINTER_OWN));
        }

        %typemap(in, numinputs=0) RegionCreator** (RegionCreator* temp){
          temp = NULL;
          $1 = &temp;
        }
        %typemap(argout) RegionCreator** {
          %append_output(SWIG_NewPointerObj(%as_voidptr(*$1), $*1_descriptor, SWIG_POINTER_OWN));
        }

        %typemap(in, numinputs=0) VectorClient** (VectorClient* temp){
          temp = NULL;
          $1 = &temp;
        }
        %typemap(argout) VectorClient** {
          %append_output(SWIG_NewPointerObj(%as_voidptr(*$1), $*1_descriptor, SWIG_POINTER_OWN));
        }

        %typemap(in, numinputs=0) VectorIndexCreator** (VectorIndexCreator* temp){
          temp = NULL;
          $1 = &temp;
        }
        %typemap(argout) VectorIndexCreator** {
          %append_output(SWIG_NewPointerObj(%as_voidptr(*$1), $*1_descriptor, SWIG_POINTER_OWN));
        }

        %typemap(in, numinputs=0) std::vector<SearchResult>& (std::vector<SearchResult> temp) {
          $1 = &temp;
        }
        %typemap(argout) std::vector<SearchResult>& {
            PyObject* obj = PyList_New($1->size());
            for (size_t i = 0; i < $1->size(); i++) {
                PyObject* item = SWIG_NewPointerObj(new SearchResult((*$1)[i]), $descriptor(dingodb::sdk::SearchResult*), SWIG_POINTER_OWN);
                PyList_SetItem(obj, i, item);
            }
            $result = SWIG_AppendOutput($result, obj);
        }

        %typemap(in, numinputs=0) std::vector<DeleteResult>& (std::vector<DeleteResult> temp) {
          $1 = &temp;
        }
        %typemap(argout) std::vector<DeleteResult>& {
          PyObject* obj = PyList_New($1->size());
          for (size_t i = 0; i < $1->size(); i++) {
            PyObject* item = SWIG_NewPointerObj(new DeleteResult((*$1)[i]), $descriptor(dingodb::sdk::DeleteResult*), SWIG_POINTER_OWN);
            PyList_SetItem(obj, i, item);
          }
          $result = SWIG_AppendOutput($result, obj);
        }

        %typemap(in, numinputs=0) QueryResult& (QueryResult temp) {
          $1 = &temp;
        }
        %typemap(argout) QueryResult& {
          PyObject* obj = SWIG_NewPointerObj(new QueryResult(*$1), $1_descriptor, SWIG_POINTER_OWN);
          $result = SWIG_AppendOutput($result, obj);
        }

        %typemap(in, numinputs=0) ScanQueryResult& (ScanQueryResult temp) {
          $1 = &temp;
        }
        %typemap(argout) ScanQueryResult& {
          PyObject* obj = SWIG_NewPointerObj(new ScanQueryResult(*$1), $1_descriptor, SWIG_POINTER_OWN);
          $result = SWIG_AppendOutput($result, obj);
        }

        %typemap(in, numinputs=0) IndexMetricsResult& (IndexMetricsResult temp) {
          $1 = &temp;
        }
        %typemap(argout) IndexMetricsResult& {
          PyObject* obj = SWIG_NewPointerObj(new IndexMetricsResult(*$1), $1_descriptor, SWIG_POINTER_OWN);
          $result = SWIG_AppendOutput($result, obj);
        }

    }

}


%include "sdk/status.h"
%include "sdk/vector.h"
%include "sdk/client.h"
