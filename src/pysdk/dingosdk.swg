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
// %include <std_unique_ptr.i> swig 4.0
%include <std_shared_ptr.i>
%include <typemaps.i>

%naturalvar;
%feature("autodoc", "3");

%ignore operator[];
%ignore operator=;

%template(KVPairVector) std::vector<dingodb::sdk::KVPair>;
%template(KeyOpStateVector) std::vector<dingodb::sdk::KeyOpState>;

%template(VectorWithIdVector)  std::vector<dingodb::sdk::VectorWithId>;
%template(SearchResultVector)  std::vector<dingodb::sdk::SearchResult>;
%template(DeleteResultVector)  std::vector<dingodb::sdk::DeleteResult>;

%template(Int64Vector)  std::vector<int64_t>; 
%template(FloatValueVector) std::vector<float>;
%template(StringVector) std::vector<std::string>;

// %apply uint8_t { dingodb::sdk::SearchExtraParamType };

// client
%apply int64_t& OUTPUT  { int64_t& out_index_id };
%apply bool& OUTPUT { bool& out_create_in_progress };

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

        %feature("docstring") RawKV::Get "return Status, std::string out_value"
        %feature("docstring") RawKV::PutIfAbsent "return Status, bool out_state"
        %feature("docstring") RawKV::DeleteRangeNonContinuous "return Status, int64_t& out_delete_count"
        %feature("docstring") RawKV::DeleteRange "return Status, int64_t out_delete_count"
        %feature("docstring") RawKV::CompareAndSet "return Status, bool out_state"

        %feature("docstring") RegionCreator::Create "return Status, int64_t out_region_id"

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
    }

}


%include "sdk/status.h"
%include "sdk/vector.h"
%include "sdk/client.h"
