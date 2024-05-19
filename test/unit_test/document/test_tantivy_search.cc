// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <sys/types.h>

#include <filesystem>

#include "document/codec.h"
#include "tantivy_search.h"

static size_t log_level = 1;

const std::string kTantivySearchTestIndexPath = "./tantivy_test_index";
const std::string kTantivySearchTestLogPath = "./tantivy_test_log";

const std::string kStemJson =
    R"({"col1": {"tokenizer": {"type": "stem", "stop_word_filters": ["english", "french"], "stem_languages": ["german", "english"], "length_limit": 60}}, "col2": {"tokenizer": {"type": "simple"}}})";

const std::string kRawJson =
    R"({ "col1": { "tokenizer": { "type": "raw" } }, "col2": { "tokenizer": {"type": "raw"} }, "col3": { "tokenizer": {"type": "raw"} } })";

const std::string kSimpleJson =
    R"lit({ "mapKeys(col1)": { "tokenizer": { "type": "stem", "stop_word_filters": ["english"], "stem_languages": ["english"]} }, "col2": { "tokenizer": {"type": "simple"} } })lit";

const std::string kSimpleJson2 =
    R"({ "col1": { "tokenizer": { "type": "stem", "stop_word_filters": ["english"], "stem_languages": ["english"]} }, "col2": { "tokenizer": {"type": "simple"} } })";

const std::string kChineseJson = R"({"text":{"tokenizer":{"type":"chinese"}}})";

const std::string kMultiTypeColumnJson =
    R"({"col1": { "tokenizer": { "type": "chinese"}}, "col2": { "tokenizer": {"type": "i64", "indexed": true }}, "col3": { "tokenizer": {"type": "f64", "indexed": true }}, "col4": { "tokenizer": {"type": "chinese"}} })";

const std::string kBytesColumnJson =
    R"({"col1": { "tokenizer": { "type": "chinese"}}, "col2": { "tokenizer": {"type": "i64", "indexed": true }}, "col3": { "tokenizer": {"type": "f64", "indexed": true }}, "col4": { "tokenizer": {"type": "chinese"}}, "col5": { "tokenizer": {"type": "bytes", "indexed": true }} })";

class DingoTantivySearchTest : public testing::Test {
 protected:
  void SetUp() override {
    // print test start info and current path
    std::cout << "tantivy_search test start, current_path: " << std::filesystem::current_path() << '\n';
    std::filesystem::remove_all(kTantivySearchTestIndexPath);
    std::filesystem::remove_all(kTantivySearchTestLogPath);
  }
  void TearDown() override {
    // remove kTantivySearchTestIndexPath and kTantivySearchTestLogPath
    std::filesystem::remove_all(kTantivySearchTestIndexPath);
    std::filesystem::remove_all(kTantivySearchTestLogPath);

    // print test end and current path
    std::cout << "tantivy_search test end, current_path: " << std::filesystem::current_path() << '\n';
  }
};

TEST(DingoTantivySearchTest, test_default_create) {
  std::filesystem::remove_all(kTantivySearchTestIndexPath);
  tantivy_search_log4rs_initialize(kTantivySearchTestLogPath.c_str(), "info", true, false, false);

  std::string index_path{kTantivySearchTestIndexPath};
  std::vector<std::string> column_names;
  column_names.push_back("text");
  auto ret = ffi_create_index(index_path, column_names);
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_multi_column_docs(index_path, 0, {"text"},
                                    {"Ancient empires rise and fall, shaping history's course."});
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_multi_column_docs(index_path, 1, {"text"},
                                    {"Artistic expressions reflect diverse cultural heritages."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 2, {"text"},
                                    {"Social movements transform societies, forging new paths."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 3, {"text"},
                                    {"Economies fluctuate, reflecting the complex "
                                     "interplay of global forces."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 4, {"text"},
                                    {"Strategic military campaigns alter the balance of power."});
  EXPECT_EQ(ret.result, true);
  ret =
      ffi_index_multi_column_docs(index_path, 5, {"text"}, {"Quantum leaps redefine understanding of physical laws."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 6, {"text"}, {"Chemical reactions unlock mysteries of nature."});
  EXPECT_EQ(ret.result, true);
  ret =
      ffi_index_multi_column_docs(index_path, 7, {"text"}, {"Philosophical debates ponder the essence of existence."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 8, {"text"}, {"Marriages blend traditions, celebrating love's union."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 9, {"text"},
                                    {"Explorers discover uncharted territories, expanding world maps."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 10, {"text"}, {"Innovations in technology drive societal progress."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 11, {"text"},
                                    {"Environmental conservation efforts protect Earth's biodiversity."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 12, {"text"},
                                    {"Diplomatic negotiations seek to resolve international conflicts."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 13, {"text"},
                                    {"Ancient philosophies provide wisdom for modern dilemmas."});
  EXPECT_EQ(ret.result, true);
  ret =
      ffi_index_multi_column_docs(index_path, 14, {"text"}, {"Economic theories debate the merits of market systems."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 15, {"text"},
                                    {"Military strategies evolve with technological advancements."});
  EXPECT_EQ(ret.result, true);
  ret =
      ffi_index_multi_column_docs(index_path, 16, {"text"}, {"Physics theories delve into the universe's mysteries."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 17, {"text"},
                                    {"Chemical compounds play crucial roles in medical breakthroughs."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 18, {"text"},
                                    {"Philosophers debate ethics in the age of artificial intelligence."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 19, {"text"},
                                    {"Wedding ceremonies across cultures symbolize lifelong commitment."});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_writer_commit(index_path);
  EXPECT_EQ(ret.result, true);

  ret = ffi_load_index_reader(index_path);
  EXPECT_EQ(ret.result, true);

  auto result = ffi_bm25_search(index_path, "of", 10, {}, false).result;

  for (const auto& it : result) {
    std::cout << "rowid:" << it.row_id << " score:" << it.score << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id
              << '\n';
  }

  ret = ffi_free_index_reader(index_path);
  EXPECT_EQ(ret.result, true);
  ret = ffi_free_index_writer(index_path);
  EXPECT_EQ(ret.result, true);

  std::cout << __func__ << " done" << '\n';
}

TEST(DingoTantivySearchTest, test_get_index_info) {
  std::filesystem::remove_all(kTantivySearchTestIndexPath);
  tantivy_search_log4rs_initialize(kTantivySearchTestLogPath.c_str(), "info", true, false, false);

  std::string index_path{kTantivySearchTestIndexPath};
  std::vector<std::string> column_names;
  column_names.push_back("text");
  auto ret = ffi_create_index_with_parameter(index_path, column_names, kMultiTypeColumnJson);
  EXPECT_EQ(ret.result, true);
  ret = ffi_free_index_writer(index_path);
  EXPECT_EQ(ret.result, true);

  auto index_meta = ffi_get_index_meta_json(index_path);
  if (index_meta.error_code != 0) {
    std::cout << "ffi_get_index_meta_json error:" << index_meta.error_msg.c_str() << '\n';
  } else {
    std::cout << "ffi_get_index_meta_json success:" << index_meta.result.c_str() << '\n';
  }

  auto index_para = ffi_get_index_json_parameter(index_path);
  if (index_para.error_code != 0) {
    std::cout << "ffi_get_index_json_parameter error:" << index_para.error_msg.c_str() << '\n';
  } else {
    std::cout << "ffi_get_index_json_parameter success:" << index_para.result.c_str() << '\n';
  }
}

TEST(DingoTantivySearchTest, test_tokenizer_create) {
  std::filesystem::remove_all(kTantivySearchTestIndexPath);
  tantivy_search_log4rs_initialize(kTantivySearchTestLogPath.c_str(), "info", true, false, false);

  std::string index_path{kTantivySearchTestIndexPath};
  std::vector<std::string> column_names;
  column_names.push_back("text");
  auto ret = ffi_create_index_with_parameter(index_path, column_names, kChineseJson);
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_multi_column_docs(index_path, 0, {"text"},
                                    {"古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭"
                                     "刻了时代的变迁与文明的发展。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 1, {"text"},
                                    {"艺术的多样表达方式反映了不同文化的丰富遗产，展"
                                     "现了人类创造力的无限可能。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 2, {"text"},
                                    {"社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 3, {"text"},
                                    {"全球经济的波动复杂多变，如同镜子反映出世界各国"
                                     "之间错综复杂的力量关系。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 4, {"text"},
                                    {"战略性的军事行动改变了世界的权力格局，也重新定义了国际政治的均势。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 5, {"text"},
                                    {"量子物理的飞跃性进展，彻底改写了我们对物理世界规律的理解和认知。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 6, {"text"},
                                    {"化学反应不仅揭开了大自然奥秘的一角，也为科学的探索提供了新的窗口。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 7, {"text"},
                                    {"哲学家的辩论深入探讨了生命存在的本质，引发人们对生存意义的深刻思考。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 8, {"text"},
                                    {"婚姻的融合不仅是情感的结合，更是不同传统和文化"
                                     "的交汇，彰显了爱的力量。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 9, {"text"},
                                    {"勇敢的探险家发现了未知的领域，为人类的世界观增添了新的地理篇章。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 10, {"text"},
                                    {"科技创新的步伐从未停歇，它推动着社会的进步，引领着时代的前行。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 11, {"text"},
                                    {"环保行动积极努力保护地球的生物多样性，为我们共"
                                     "同的家园筑起绿色的屏障。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 12, {"text"},
                                    {"外交谈判在国际舞台上寻求和平解决冲突，致力于构建一个更加和谐的世界。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 13, {"text"},
                                    {"古代哲学的智慧至今仍对现代社会的诸多难题提供启示和解答，影响深远。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 14, {"text"},
                                    {"经济学理论围绕市场体系的优劣进行了深入的探讨与"
                                     "辩论，对经济发展有重要指导意义。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 15, {"text"},
                                    {"随着科技的不断进步，军事战略也在不断演变，应对新时代的挑战和需求。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 16, {"text"},
                                    {"现代物理学理论深入挖掘宇宙的奥秘，试图解开那些探索宇宙时的未知之谜。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 17, {"text"},
                                    {"在医学领域，化学化合物的作用至关重要，它们在许"
                                     "多重大医疗突破中扮演了核心角色。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 18, {"text"},
                                    {"当代哲学家在探讨人工智能时代的伦理道德问题，对"
                                     "机器与人类的关系进行深刻反思。"});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_column_docs(index_path, 19, {"text"},
                                    {"不同文化背景下的婚礼仪式代表着一生的承诺与责任"
                                     "，象征着两颗心的永恒结合。"});
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_writer_commit(index_path);
  EXPECT_EQ(ret.result, true);

  ret = ffi_load_index_reader(index_path);
  EXPECT_EQ(ret.result, true);

  auto result = ffi_bm25_search(index_path, "影响深远", 10, {}, false).result;

  for (const auto& it : result) {
    std::cout << "rowid:" << it.row_id << " score:" << it.score << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id
              << '\n';
  }

  ret = ffi_free_index_reader(index_path);
  EXPECT_EQ(ret.result, true);
  ret = ffi_free_index_writer(index_path);
  EXPECT_EQ(ret.result, true);

  std::cout << __func__ << " done" << '\n';
}

void CreateAndLoadChineseData(const std::string& index_path) {
  std::filesystem::remove_all(index_path);
  tantivy_search_log4rs_initialize(kTantivySearchTestLogPath.c_str(), "info", true, false, false);

  std::vector<std::string> column_names;
  column_names.push_back("col1");
  column_names.push_back("col2");
  column_names.push_back("col3");
  column_names.push_back("col4");

  std::vector<std::string> text_column_names;
  text_column_names.push_back("col1");
  text_column_names.push_back("col4");

  std::vector<std::string> i64_column_names;
  i64_column_names.push_back("col2");

  std::vector<std::string> f64_column_names;
  f64_column_names.push_back("col3");

  auto ret = ffi_create_index_with_parameter(index_path, column_names, kMultiTypeColumnJson);
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_multi_type_column_docs(index_path, 0, {"col1", "col4"},
                                         {"古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭"
                                          "刻了时代的变迁与文明的发展。",
                                          "Ancient empires rise and fall, shaping history's course."},
                                         {"col2"}, {100}, {"col3"}, {100.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 1, text_column_names,
                                         {"艺术的多样表达方式反映了不同文化的丰富遗产，展现了人类创造力的无限可能"
                                          "。",
                                          "Artistic expressions reflect diverse cultural heritages."},
                                         i64_column_names, {200}, f64_column_names, {200.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 2, text_column_names,
                                         {"社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。",
                                          "Social movements transform societies, forging new paths."},
                                         i64_column_names, {300}, f64_column_names, {300.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 3, text_column_names,
                                         {"全球经济的波动复杂多变，如同镜子反映出世界各国之间错综复杂的力量关系。",
                                          "Global economic fluctuations are complex and volatile, reflecting "
                                          "intricate power dynamics among nations."},
                                         i64_column_names, {400}, f64_column_names, {400.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 4, text_column_names,
                                         {"战略性的军事行动改变了世界的权力格局，也重新定义了国际政治的均势。",
                                          "Strategic military campaigns alter the balance of power."},
                                         i64_column_names, {500}, f64_column_names, {500.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 5, text_column_names,
                                         {"量子物理的飞跃性进展，彻底改写了我们对物理世界规律的理解和认知。",
                                          "Quantum leaps redefine understanding of physical laws."},
                                         i64_column_names, {600}, f64_column_names, {600.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 6, text_column_names,
                                         {"化学反应不仅揭开了大自然奥秘的一角，也为科学的探索提供了新的窗口。",
                                          "Chemical reactions unlock mysteries of nature."},
                                         i64_column_names, {700}, f64_column_names, {700.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 7, text_column_names,
                                         {"哲学家的辩论深入探讨了生命存在的本质，引发人们对生存意义的深刻思考。",
                                          "Philosophical debates ponder the essence of existence."},
                                         i64_column_names, {800}, f64_column_names, {800.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(
      index_path, 8, text_column_names,
      {"婚姻的融合不仅是情感的结合，更是不同传统和文化的交汇，彰显了爱的力量,是社会发展的必须。",
       "Marriages blend traditions, celebrating love's union."},
      i64_column_names, {900}, f64_column_names, {900.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 9, text_column_names,
                                         {"勇敢的探险家发现了未知的领域，为人类的世界观增添了新的地理篇章。",
                                          "Brave explorers discover uncharted territories, expanding world maps."},
                                         i64_column_names, {1000}, f64_column_names, {1000.0}, {}, {});
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_writer_commit(index_path);
  EXPECT_EQ(ret.result, true);
  ret = ffi_free_index_writer(index_path);
  EXPECT_EQ(ret.result, true);
}

TEST(DingoTantivySearchTest, test_multi_type_column) {
  std::filesystem::remove_all(kTantivySearchTestIndexPath);
  tantivy_search_log4rs_initialize(kTantivySearchTestLogPath.c_str(), "info", true, false, false);

  std::string index_path{kTantivySearchTestIndexPath};
  std::vector<std::string> column_names;
  column_names.push_back("col1");
  column_names.push_back("col2");
  column_names.push_back("col3");
  column_names.push_back("col4");

  std::vector<std::string> text_column_names;
  text_column_names.push_back("col1");
  text_column_names.push_back("col4");

  std::vector<std::string> i64_column_names;
  i64_column_names.push_back("col2");

  std::vector<std::string> f64_column_names;
  f64_column_names.push_back("col3");

  auto ret = ffi_create_index_with_parameter(index_path, column_names, kMultiTypeColumnJson);
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_multi_type_column_docs(index_path, 0, {"col1", "col4"},
                                         {"古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭"
                                          "刻了时代的变迁与文明的发展。",
                                          "Ancient empires rise and fall, shaping history's course."},
                                         {"col2"}, {100}, {"col3"}, {100.0}, {}, {});
  std::cout << "ffi_index_multi_type_column_docs ret:" << ret.result << '\n';
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 1, text_column_names,
                                         {"艺术的多样表达方式反映了不同文化的丰富遗产，展现了人类创造力的无限可能"
                                          "。",
                                          "Artistic expressions reflect diverse cultural heritages."},
                                         i64_column_names, {200}, f64_column_names, {200.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 2, text_column_names,
                                         {"社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。",
                                          "Social movements transform societies, forging new paths."},
                                         i64_column_names, {300}, f64_column_names, {300.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 3, text_column_names,
                                         {"全球经济的波动复杂多变，如同镜子反映出世界各国之间错综复杂的力量关系。",
                                          "Global economic fluctuations are complex and volatile, reflecting "
                                          "intricate power dynamics among nations."},
                                         i64_column_names, {400}, f64_column_names, {400.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 4, text_column_names,
                                         {"战略性的军事行动改变了世界的权力格局，也重新定义了国际政治的均势。",
                                          "Strategic military campaigns alter the balance of power."},
                                         i64_column_names, {500}, f64_column_names, {500.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 5, text_column_names,
                                         {"量子物理的飞跃性进展，彻底改写了我们对物理世界规律的理解和认知。",
                                          "Quantum leaps redefine understanding of physical laws."},
                                         i64_column_names, {600}, f64_column_names, {600.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 6, text_column_names,
                                         {"化学反应不仅揭开了大自然奥秘的一角，也为科学的探索提供了新的窗口。",
                                          "Chemical reactions unlock mysteries of nature."},
                                         i64_column_names, {700}, f64_column_names, {700.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 7, text_column_names,
                                         {"哲学家的辩论深入探讨了生命存在的本质，引发人们对生存意义的深刻思考。",
                                          "Philosophical debates ponder the essence of existence."},
                                         i64_column_names, {800}, f64_column_names, {800.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(
      index_path, 8, text_column_names,
      {"婚姻的融合不仅是情感的结合，更是不同传统和文化的交汇，彰显了爱的力量,是社会发展的必须。",
       "Marriages blend traditions, celebrating love's union."},
      i64_column_names, {900}, f64_column_names, {900.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_multi_type_column_docs(index_path, 9, text_column_names,
                                         {"勇敢的探险家发现了未知的领域，为人类的世界观增添了新的地理篇章。",
                                          "Brave explorers discover uncharted territories, expanding world maps."},
                                         i64_column_names, {1000}, f64_column_names, {1000.0}, {}, {});
  EXPECT_EQ(ret.result, true);
  ret = ffi_index_writer_commit(index_path);
  EXPECT_EQ(ret.result, true);

  ret = ffi_load_index_reader(index_path);
  EXPECT_EQ(ret.result, true);

  auto result = ffi_bm25_search(index_path, "社会", 10, {}, false).result;
  std::cout << "ffi_bm25_search result size:" << result.size() << '\n';

  for (const auto& it : result) {
    std::cout << "ffi_bm25_search rowid:" << it.row_id << " score:" << it.score << " doc_id:" << it.doc_id
              << " seg_id:" << it.seg_id << '\n';
  }

  result = ffi_bm25_search_with_column_names(index_path, "社会", 10, {}, false, false, 0, 0, {"col1"}).result;
  std::cout << "ffi_bm25_search_with_column_names col1 result size:" << result.size() << '\n';
  for (const auto& it : result) {
    std::cout << "ffi_bm25_search_with_column_names rowid:" << it.row_id << " score:" << it.score
              << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
  }

  result = ffi_bm25_search_with_column_names(index_path, "balance", 10, {}, false, false, 0, 0, {"col4"}).result;
  std::cout << "ffi_bm25_search_with_column_names col4 result size:" << result.size() << '\n';
  for (const auto& it : result) {
    std::cout << "ffi_bm25_search_with_column_names rowid:" << it.row_id << " score:" << it.score
              << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
  }

  result = ffi_bm25_search_with_column_names(index_path, "社会", 10, {}, false, false, 0, 0, {"col1", "col4"}).result;
  std::cout << "ffi_bm25_search_with_column_names col1,col4 result size:" << result.size() << '\n';
  for (const auto& it : result) {
    std::cout << "ffi_bm25_search_with_column_names rowid:" << it.row_id << " score:" << it.score
              << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
  }

  result = ffi_bm25_search_with_column_names(index_path, "社会", 10, {}, false, false, 0, 0, {"col11", "col44"}).result;
  std::cout << "ffi_bm25_search_with_column_names col11,col44 result size:" << result.size() << '\n';

  result =
      ffi_bm25_search_with_column_names(index_path, "col2: IN [200 300 400]", 10, {}, false, false, 0, 0, {}).result;
  std::cout << "ffi_bm25_search_with_column_names-1 parser result size:" << result.size() << '\n';
  for (const auto& it : result) {
    std::cout << "ffi_bm25_search_with_column_names rowid:" << it.row_id << " score:" << it.score
              << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
  }

  result =
      ffi_bm25_search_with_column_names(index_path, "col222: IN [200 300 400]", 10, {}, false, false, 0, 0, {}).result;
  std::cout << "ffi_bm25_search_with_column_names-2 parser result size:" << result.size() << '\n';
  for (const auto& it : result) {
    std::cout << "ffi_bm25_search_with_column_names rowid:" << it.row_id << " score:" << it.score
              << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
  }

  auto bm25_result =
      ffi_bm25_search_with_column_names(index_path, "col2: IN [200 300 400]", 10, {}, false, false, 0, 0, {});
  if (bm25_result.error_code != 0) {
    std::cout << "ffi_bm25_search_with_column_names2-1 error:" << bm25_result.error_msg.c_str() << '\n';
  } else {
    std::cout << "ffi_bm25_search_with_column_names2-1 parser result size:" << bm25_result.result.size() << '\n';
    for (const auto& it : bm25_result.result) {
      std::cout << "ffi_bm25_search_with_column_names2 rowid:" << it.row_id << " score:" << it.score
                << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
    }
  }

  bm25_result = ffi_bm25_search_with_column_names(index_path, "col222: IN [200 300 400 500 600 700 800]", 10, {}, false,
                                                  false, 0, 0, {});
  if (bm25_result.error_code != 0) {
    std::cout << "ffi_bm25_search_with_column_names2-2 error:" << bm25_result.error_msg.c_str() << '\n';
  } else {
    std::cout << "ffi_bm25_search_with_column_names2-2 parser result size:" << bm25_result.result.size() << '\n';
    for (const auto& it : bm25_result.result) {
      std::cout << "ffi_bm25_search_with_column_names2 rowid:" << it.row_id << " score:" << it.score
                << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
    }
  }

  auto reload_result = ffi_index_reader_reload(index_path);
  if (reload_result.error_code != 0) {
    std::cout << "ffi_index_reader_reload error:" << reload_result.error_msg.c_str() << '\n';
  } else {
    std::cout << "ffi_index_reader_reload success" << '\n';
  }

  std::vector<uint64_t> alived_ids;
  alived_ids.push_back(2);
  alived_ids.push_back(4);
  alived_ids.push_back(5);
  alived_ids.push_back(6);
  alived_ids.push_back(7);
  bm25_result = ffi_bm25_search_with_column_names(index_path, "col2: IN [800 700 600 500 400 300 200]", 3, alived_ids,
                                                  true, false, 0, 0, {});
  if (bm25_result.error_code != 0) {
    std::cout << "ffi_bm25_search_with_column_names2-3 filter_ids error:" << bm25_result.error_msg.c_str() << '\n';
  } else {
    std::cout << "ffi_bm25_search_with_column_names2-3 filter_ids parser result size:" << bm25_result.result.size()
              << '\n';
    for (const auto& it : bm25_result.result) {
      std::cout << "ffi_bm25_search_with_column_names2 rowid:" << it.row_id << " score:" << it.score
                << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
    }
  }

  ret = ffi_free_index_reader(index_path);
  EXPECT_EQ(ret.result, true);
  ret = ffi_free_index_writer(index_path);
  EXPECT_EQ(ret.result, true);

  std::cout << __func__ << " done" << '\n';
}

TEST(DingoTantivySearchTest, test_load_multi_type_column) {
  std::string index_path{kTantivySearchTestIndexPath};
  CreateAndLoadChineseData(index_path);

  tantivy_search_log4rs_initialize(kTantivySearchTestLogPath.c_str(), "info", true, false, false);

  BoolResult bool_result;
  bool_result = ffi_load_index_reader(index_path);
  if (!bool_result.result) {
    std::cout << "ffi_load_index_reader error:" << bool_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bool_result.result, true);
    return;
  } else {
    std::cout << "ffi_load_index_reader success" << '\n';
  }

  bool_result = ffi_load_index_writer(index_path);
  if (!bool_result.result) {
    std::cout << "ffi_load_index_writer error:" << bool_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bool_result.result, true);
    return;
  } else {
    std::cout << "ffi_load_index_writer success" << '\n';
  }

  bool_result = ffi_index_multi_type_column_docs(index_path, 0, {"col1", "col4"},
                                                 {"古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭"
                                                  "刻了时代的变迁与文明的发展。",
                                                  "Ancient empires rise and fall, shaping history's course."},
                                                 {"col2"}, {101}, {}, {}, {}, {});
  if (!bool_result.result) {
    std::cout << "ffi_index_multi_type_column_docs error:" << bool_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bool_result.result, true);
  } else {
    std::cout << "ffi_index_multi_type_column_docs ret:" << bool_result.result << '\n';
  }

  bool_result = ffi_index_writer_commit(index_path);
  if (!bool_result.result) {
    std::cout << "ffi_index_writer_commit error:" << bool_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bool_result.result, true);
  } else {
    std::cout << "ffi_index_writer_commit success" << '\n';
  }

  bool_result = ffi_index_reader_reload(index_path);
  if (!bool_result.result) {
    std::cout << "ffi_index_reader_reload error:" << bool_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bool_result.result, true);
  } else {
    std::cout << "ffi_index_reader_reload success" << '\n';
  }

  auto bm25_result = ffi_bm25_search_with_column_names(index_path, "col2: IN [101]", 10, {}, false, false, 0, 0, {});
  if (bm25_result.error_code != 0) {
    std::cout << "ffi_bm25_search_with_column_names2-1 error:" << bm25_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bm25_result.error_code, 0);
  } else {
    std::cout << "ffi_bm25_search_with_column_names2-1 parser result size:" << bm25_result.result.size() << '\n';
    for (const auto& it : bm25_result.result) {
      std::cout << "ffi_bm25_search_with_column_names2 rowid:" << it.row_id << " score:" << it.score
                << " doc_id:" << it.doc_id << " seg_id:" << it.seg_id << '\n';
    }
  }

  bool_result = ffi_free_index_reader(index_path);
  if (!bool_result.result) {
    std::cout << "ffi_free_index_reader error:" << bool_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bool_result.result, true);
  } else {
    std::cout << "ffi_free_index_reader success" << '\n';
  }

  bool_result = ffi_free_index_writer(index_path);
  if (!bool_result.result) {
    std::cout << "ffi_free_index_writer error:" << bool_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bool_result.result, true);
  } else {
    std::cout << "ffi_free_index_writer success" << '\n';
  }

  std::cout << __func__ << " done" << '\n';
}

TEST(DingoTantivySearchTest, test_bytes_column) {
  std::cout << __func__ << " start" << '\n';

  std::filesystem::remove_all("./temp");
  tantivy_search_log4rs_initialize("./log", "info", true, false, false);

  std::string index_path{"./temp"};
  std::vector<std::string> column_names;
  column_names.push_back("col1");
  column_names.push_back("col2");
  column_names.push_back("col3");
  column_names.push_back("col4");
  column_names.push_back("col5");  // bytes column

  std::vector<std::string> text_column_names;
  text_column_names.push_back("col1");
  text_column_names.push_back("col4");

  std::vector<std::string> i64_column_names;
  i64_column_names.push_back("col2");

  std::vector<std::string> f64_column_names;
  f64_column_names.push_back("col3");

  std::vector<std::string> bytes_column_names;
  bytes_column_names.push_back("col5");

  auto create_ret = ffi_create_index_with_parameter(index_path, column_names, kBytesColumnJson);
  EXPECT_EQ(create_ret.result, true);

  auto ret = ffi_index_multi_type_column_docs(index_path, 0, {"col1", "col4"},
                                              {"古代帝国的兴衰更迭，不仅塑造了历史的进程，也铭"
                                               "刻了时代的变迁与文明的发展。",
                                               "Ancient empires rise and fall, shaping history's course."},
                                              {"col2"}, {100}, {"col3"}, {100.0}, bytes_column_names, {"test111"});
  std::cout << "ffi_index_multi_type_column_docs ret:" << ret.result << '\n';
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_multi_type_column_docs(index_path, 1, text_column_names,
                                         {"艺术的多样表达方式反映了不同文化的丰富遗产，展现了人类创造力的无限可能"
                                          "。",
                                          "Artistic expressions reflect diverse cultural heritages."},
                                         i64_column_names, {200}, f64_column_names, {200.0}, {"col5"}, {"test111"});
  std::cout << "ffi_index_multi_type_column_docs ret:" << ret.result << '\n';
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_multi_type_column_docs(index_path, 2, {"col1", "col4"},
                                         {"社会运动如同时代的浪潮，改变着社会的面貌，为历史开辟新的道路和方向。",
                                          "Social movements transform societies, forging new paths."},
                                         i64_column_names, {300}, f64_column_names, {300.0}, {"col5"}, {"test222"});
  std::cout << "ffi_index_multi_type_column_docs ret:" << ret.result << '\n';
  EXPECT_EQ(ret.result, true);

  ret = ffi_index_writer_commit(index_path);
  EXPECT_EQ(ret.result, true);

  ret = ffi_load_index_reader(index_path);
  EXPECT_EQ(ret.result, true);

  std::vector<uint64_t> alived_ids;
  alived_ids.push_back(0);
  alived_ids.push_back(1);
  alived_ids.push_back(2);
  auto bm25_result =
      ffi_bm25_search_with_column_names(index_path, "col2: IN [100 200]", 3, alived_ids, true, false, 0, 0, {});
  if (bm25_result.error_code != 0) {
    std::cout << __func__ << "test-1 filter_ids error:" << bm25_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bm25_result.error_code, 0);
  } else {
    std::cout << __func__ << "test-1 filter_ids parser result size:" << bm25_result.result.size() << '\n';
    for (const auto& it : bm25_result.result) {
      std::cout << __func__ << "test-1 rowid:" << it.row_id << " score:" << it.score << " doc_id:" << it.doc_id
                << " seg_id:" << it.seg_id << '\n';
    }
  }

  bm25_result = ffi_bm25_search_with_column_names(index_path, "col3: > 101", 10, {}, false, false, 0, 0, {});
  if (bm25_result.error_code != 0) {
    std::cout << __func__ << "test-2 parser error:" << bm25_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bm25_result.error_code, 0);
  } else {
    std::cout << __func__ << "test-2 parser result size:" << bm25_result.result.size() << '\n';
    for (const auto& it : bm25_result.result) {
      std::cout << __func__ << "test-2 rowid:" << it.row_id << " score:" << it.score << " doc_id:" << it.doc_id
                << " seg_id:" << it.seg_id << '\n';
    }
  }

  bm25_result =
      ffi_bm25_search_with_column_names(index_path, "col5: IN [dGVzdDExMQ==]", 10, {}, false, false, 0, 0, {});
  if (bm25_result.error_code != 0) {
    std::cout << __func__ << "test-3 parser error:" << bm25_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bm25_result.error_code, 0);
  } else {
    std::cout << __func__ << "test-3 parser result size:" << bm25_result.result.size() << '\n';
    EXPECT_EQ(bm25_result.result.size(), 2);
    for (const auto& it : bm25_result.result) {
      std::cout << __func__ << "test-3 rowid:" << it.row_id << " score:" << it.score << " doc_id:" << it.doc_id
                << " seg_id:" << it.seg_id << '\n';
    }
  }

  bm25_result = ffi_bm25_search_with_column_names(index_path, "col5: IN [dGVzdDExMQ==]", 10, {}, false, true, 1, 2, {});
  if (bm25_result.error_code != 0) {
    std::cout << __func__ << "test-4 parser error:" << bm25_result.error_msg.c_str() << '\n';
    EXPECT_EQ(bm25_result.error_code, 0);
  } else {
    std::cout << __func__ << "test-4 parser result size:" << bm25_result.result.size() << '\n';
    EXPECT_EQ(bm25_result.result.size(), 1);
    for (const auto& it : bm25_result.result) {
      std::cout << __func__ << "test-4 rowid:" << it.row_id << " score:" << it.score << " doc_id:" << it.doc_id
                << " seg_id:" << it.seg_id << '\n';
    }
  }

  ret = ffi_free_index_reader(index_path);
  EXPECT_EQ(ret.result, true);
  ret = ffi_free_index_writer(index_path);
  EXPECT_EQ(ret.result, true);

  std::cout << __func__ << " done" << '\n';
}

// bool ValidateTokenizerParameter(const std::string& json_parameter) {
//   if (!nlohmann::json::accept(json_parameter)) {
//     std::cout << "json_parameter is illegal" << '\n';
//     return false;
//   }

//   nlohmann::json json = nlohmann::json::parse(json_parameter);
//   for (const auto& item : json.items()) {
//     std::cout << "key:" << item.key() << " value:" << item.value() << '\n';
//     auto tokenizer = item.value();

//     if (tokenizer.find("tokenizer") == tokenizer.end()) {
//       std::cout << "not found tokenizer" << '\n';
//       return false;
//     }

//     const auto& tokenizer_item = tokenizer.at("tokenizer");
//     std::cout << "tokenizer: " << tokenizer << '\n';

//     if (tokenizer_item.find("type") == tokenizer_item.end()) {
//       std::cout << "not found tokenizer type" << '\n';
//       return false;
//     }

//     const auto& tokenizer_type = tokenizer_item.at("type");
//     std::cout << "tokenizer_type: " << tokenizer_type << '\n';

//     if (tokenizer_type == "default" || tokenizer_type == "raw" || tokenizer_type == "simple" ||
//         tokenizer_type == "stem" || tokenizer_type == "whitespace" || tokenizer_type == "ngram" ||
//         tokenizer_type == "chinese") {
//       std::cout << "text column" << '\n';
//     } else if (tokenizer_type == "i64") {
//       std::cout << "i64 column" << '\n';
//     } else if (tokenizer_type == "f64") {
//       std::cout << "f64 column" << '\n';
//     } else if (tokenizer_type == "bytes") {
//       std::cout << "bytes column" << '\n';
//     } else {
//       std::cout << "unknown column" << '\n';
//       return false;
//     }
//   }

//   return true;
// }

TEST(DingoTantivySearchTest, test_json_parse) {
  std::string error_message;
  std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter;

  // normal json
  EXPECT_EQ(
      dingodb::DocumentCodec::IsValidTokenizerJsonParameter(
          R"({"col1": { "tokenizer": { "type": "chinese"}}, "col2": { "tokenizer": {"type": "i64", "indexed": true }}, "col3": { "tokenizer": {"type": "f64", "indexed": true }}, "col4": { "tokenizer": {"type": "chinese"}} })",
          column_tokenizer_parameter, error_message),
      true);

  EXPECT_EQ(column_tokenizer_parameter.size(), 4);
  EXPECT_EQ(column_tokenizer_parameter.at("col1"), dingodb::TokenizerType::kTokenizerTypeText);
  EXPECT_EQ(column_tokenizer_parameter.at("col4"), dingodb::TokenizerType::kTokenizerTypeText);
  EXPECT_EQ(column_tokenizer_parameter.at("col2"), dingodb::TokenizerType::kTokenizerTypeI64);
  EXPECT_EQ(column_tokenizer_parameter.at("col3"), dingodb::TokenizerType::kTokenizerTypeF64);

  // bytes json
  column_tokenizer_parameter.clear();
  error_message.clear();
  EXPECT_EQ(
      dingodb::DocumentCodec::IsValidTokenizerJsonParameter(
          R"({"col1": { "tokenizer": { "type": "chinese"}}, "col2": { "tokenizer": {"type": "i64", "indexed": true }}, "col3": { "tokenizer": {"type": "f64", "indexed": true }}, "col4": { "tokenizer": {"type": "chinese"}}, "col5": { "tokenizer": {"type": "bytes", "indexed": true }} })",
          column_tokenizer_parameter, error_message),
      true);

  EXPECT_EQ(column_tokenizer_parameter.size(), 5);
  EXPECT_EQ(column_tokenizer_parameter.at("col1"), dingodb::TokenizerType::kTokenizerTypeText);
  EXPECT_EQ(column_tokenizer_parameter.at("col4"), dingodb::TokenizerType::kTokenizerTypeText);
  EXPECT_EQ(column_tokenizer_parameter.at("col2"), dingodb::TokenizerType::kTokenizerTypeI64);
  EXPECT_EQ(column_tokenizer_parameter.at("col3"), dingodb::TokenizerType::kTokenizerTypeF64);
  EXPECT_EQ(column_tokenizer_parameter.at("col5"), dingodb::TokenizerType::kTokenizerTypeBytes);

  // illegal json
  column_tokenizer_parameter.clear();
  error_message.clear();
  EXPECT_EQ(dingodb::DocumentCodec::IsValidTokenizerJsonParameter("test", column_tokenizer_parameter, error_message),
            false);
  std::cout << "error_message:" << error_message << '\n';

  // illegal tokenizer type
  column_tokenizer_parameter.clear();
  error_message.clear();
  EXPECT_EQ(dingodb::DocumentCodec::IsValidTokenizerJsonParameter(R"({"col1": {"tokenizer": {"type": "test"}} })",
                                                                  column_tokenizer_parameter, error_message),
            false);
  std::cout << "error_message:" << error_message << '\n';
}

TEST(DingoTantivySearchTest, test_gen_default_json) {
  std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter;
  column_tokenizer_parameter.insert({"col1", dingodb::TokenizerType::kTokenizerTypeText});
  column_tokenizer_parameter.insert({"col2", dingodb::TokenizerType::kTokenizerTypeI64});
  column_tokenizer_parameter.insert({"col3", dingodb::TokenizerType::kTokenizerTypeF64});
  column_tokenizer_parameter.insert({"col4", dingodb::TokenizerType::kTokenizerTypeText});
  column_tokenizer_parameter.insert({"col5", dingodb::TokenizerType::kTokenizerTypeBytes});

  std::string error_message;
  std::string json_parameter;

  EXPECT_EQ(dingodb::DocumentCodec::GenDefaultTokenizerJsonParameter(column_tokenizer_parameter, json_parameter,
                                                                     error_message),
            true);

  std::cout << "json_parameter:" << json_parameter << '\n';

  std::map<std::string, dingodb::TokenizerType> column_tokenizer_parameter2;
  error_message.clear();

  EXPECT_EQ(
      dingodb::DocumentCodec::IsValidTokenizerJsonParameter(json_parameter, column_tokenizer_parameter2, error_message),
      true);

  EXPECT_EQ(column_tokenizer_parameter.size(), column_tokenizer_parameter2.size());

  error_message.clear();
  column_tokenizer_parameter.insert({"col6", dingodb::TokenizerType::kTokenizerTypeUnknown});
  EXPECT_EQ(dingodb::DocumentCodec::GenDefaultTokenizerJsonParameter(column_tokenizer_parameter, json_parameter,
                                                                     error_message),
            false);
  std::cout << "error_message:" << error_message << '\n';
}

TEST(DingoTantivySearchTest, GetTokenizerTypeString) {
  dingodb::DocumentCodec codec;

  EXPECT_EQ(codec.GetTokenizerTypeString(dingodb::TokenizerType::kTokenizerTypeText), "text");
  EXPECT_EQ(codec.GetTokenizerTypeString(dingodb::TokenizerType::kTokenizerTypeI64), "i64");
  EXPECT_EQ(codec.GetTokenizerTypeString(dingodb::TokenizerType::kTokenizerTypeF64), "f64");
  EXPECT_EQ(codec.GetTokenizerTypeString(dingodb::TokenizerType::kTokenizerTypeBytes), "bytes");
  EXPECT_EQ(codec.GetTokenizerTypeString(static_cast<dingodb::TokenizerType>(9999)),
            "unknown");  // Test with an invalid value
}
