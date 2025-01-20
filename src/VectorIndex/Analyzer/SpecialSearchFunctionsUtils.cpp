#include <VectorIndex/Analyzer/SpecialSearchFunctionsUtils.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/TableNode.h>
#include <Core/Settings.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <VectorIndex/Utils/CommonUtils.h>
#include <VectorIndex/Utils/VIUtils.h>
#include <VectorIndex/Interpreters/parseVSParameters.h>

#if USE_TANTIVY_SEARCH
#    include <Interpreters/TantivyFilter.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_AGGREGATION;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TEXT_SEARCH;
    extern const int ILLEGAL_HYBRID_SEARCH;
}

namespace
{

class CollectHybridSearchFunctionNodesVisitor : public ConstInDepthQueryTreeVisitor<CollectHybridSearchFunctionNodesVisitor>
{
public:
    explicit CollectHybridSearchFunctionNodesVisitor(QueryTreeNodes * hybrid_function_nodes_)
        : hybrid_function_nodes(hybrid_function_nodes_)
    {}

    explicit CollectHybridSearchFunctionNodesVisitor(String assert_no_hybrids_place_message_)
        : assert_no_hybrids_place_message(std::move(assert_no_hybrids_place_message_))
    {}

    explicit CollectHybridSearchFunctionNodesVisitor(bool only_check_)
        : only_check(only_check_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (only_check && has_hybrid_search_functions)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isHybridSearchFunc(function_node->getFunctionName()))
            return;

        if (!assert_no_hybrids_place_message.empty())
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Hybrid search function {} is found {} in query",
                function_node->formatASTForErrorMessage(),
                assert_no_hybrids_place_message);

        String full_name = function_node->formatASTForErrorMessage();
        if (uniq_names.count(full_name))
            return;

        uniq_names.insert(full_name);
        if (hybrid_function_nodes)
            hybrid_function_nodes->push_back(node);

        has_hybrid_search_functions = true;
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node) const
    {
        if (only_check && has_hybrid_search_functions)
            return false;

        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

    bool hasHybridSearchFunctions() const
    {
        return has_hybrid_search_functions;
    }

private:
    String assert_no_hybrids_place_message;
    QueryTreeNodes * hybrid_function_nodes = nullptr;
    bool only_check = false;
    bool has_hybrid_search_functions = false;
    std::unordered_set<String> uniq_names {};
};

}

QueryTreeNodes collectHybridSearchFunctionNodes(const QueryTreeNodePtr & node)
{
    QueryTreeNodes result;
    CollectHybridSearchFunctionNodesVisitor visitor(&result);
    visitor.visit(node);

    return result;
}

void collectHybridSearchFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    CollectHybridSearchFunctionNodesVisitor visitor(&result);
    visitor.visit(node);
}

bool hasHybridSearchFunctionNodes(const QueryTreeNodePtr & node)
{
    CollectHybridSearchFunctionNodesVisitor visitor(true /*only_check*/);
    visitor.visit(node);

    return visitor.hasHybridSearchFunctions();
}

void assertNoHybridSearchFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_hybrids_place_message)
{
    CollectHybridSearchFunctionNodesVisitor visitor(assert_no_hybrids_place_message);
    visitor.visit(node);
}

inline void checkTantivyIndex([[maybe_unused]]const StorageMetadataPtr & metadata_snapshot, [[maybe_unused]]const String & text_column_name)
{
    bool find_tantivy_index = false;
#if USE_TANTIVY_SEARCH
    if (metadata_snapshot)
    {
        for (const auto & index_desc : metadata_snapshot->getSecondaryIndices())
        {
            /// Find tantivy inverted index on the search column
            if (index_desc.type == TANTIVY_INDEX_NAME)
            {
                auto & column_names = index_desc.column_names;
                /// Support search on a column in a multi-columns index
                if (std::find(column_names.begin(), column_names.end(), text_column_name) != column_names.end())
                {
                    find_tantivy_index = true;
                    break;
                }
            }
        }
    }
#endif
    if (!find_tantivy_index)
    {
        throw Exception(ErrorCodes::ILLEGAL_TEXT_SEARCH, "The column {} has no fts index for text search", text_column_name);
    }
}

std::pair<String, bool> getVectorIndexTypeAndParameterCheck(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, String & search_column_name)
{
    auto log = getLogger("getVectorIndexTypeAndParameterCheck");
    String index_type = "";
    /// Obtain the default value of the `use_parameter_check` in the MergeTreeSetting.
    std::unique_ptr<MergeTreeSettings> storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());
    bool use_parameter_check = storage_settings->vector_index_parameter_check;
    LOG_TRACE(log, "vector_index_parameter_check value in MergeTreeSetting: {}", use_parameter_check);

    /// Obtain the type of the vector index recorded in the meta_data.
    if (metadata_snapshot)
    {
        /// Support multiple vector indices
        /// Find vector index description in metadata based on search column name.
        for (auto & vec_index_desc : metadata_snapshot->getVectorIndices())
        {
            if (vec_index_desc.column == search_column_name)
            {
                index_type = vec_index_desc.type;
                LOG_TRACE(log, "The vector index type used for the query is `{}`", Poco::toUpper(index_type));

                break;
            }
        }

        /// If not found, brute force search will be used.
    }

    /// Use the user-defined `vector_index_parameter_check`.
    if (metadata_snapshot && metadata_snapshot->hasSettingsChanges())
    {
        const auto current_changes = metadata_snapshot->getSettingsChanges()->as<const ASTSetQuery &>().changes;
        for (const auto & changed_setting : current_changes)
        {
            const auto & setting_name = changed_setting.name;
            const auto & new_value = changed_setting.value;
            if (setting_name == "vector_index_parameter_check")
            {
                use_parameter_check = new_value.safeGet<bool>();
                LOG_TRACE(
                    log, "vector_index_parameter_check value in sql definition: {}", use_parameter_check);
                break;
            }
        }
    }

    return std::make_pair(index_type, use_parameter_check);
}

/// Fill in dim and recognize VectorSearchType from metadata
void getAndCheckVectorScanInfoFromMetadata(
    StorageMetadataPtr metadata_snapshot,
    VSDescription & vector_scan_desc,
    ContextPtr context)
{
    if (metadata_snapshot)
    {
        /// vector column dim
        vector_scan_desc.search_column_dim = VectorIndex::getVectorDimension(vector_scan_desc.vector_search_type, *metadata_snapshot, vector_scan_desc.search_column_name);
        checkVectorDimension(vector_scan_desc.vector_search_type, vector_scan_desc.search_column_dim);

        /// Parameter check
        std::pair<String, bool> res = getVectorIndexTypeAndParameterCheck(metadata_snapshot, context, vector_scan_desc.search_column_name);

        /// parse vector scan's params, such as: top_k, n_probe ...
        String param_str = parseVectorScanParameters(vector_scan_desc.parameters, Poco::toUpper(res.first), res.second);
        if (!param_str.empty())
        {
            try
            {
                Poco::JSON::Parser json_parser;
                vector_scan_desc.vector_parameters = json_parser.parse(param_str).extract<Poco::JSON::Object::Ptr>();
            }
            catch ([[maybe_unused]] const std::exception & e)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The input JSON's format is illegal");
            }
        }
    }
}

/// create vector scan description, used by HybridSearch and VectorScan
VSDescription commonMakeVectorScanDescription(
    const String & function_col_name,
    QueryTreeNodePtr query_column,
    QueryTreeNodePtr query_vector,
    int topk,
    const ContextPtr & context)
{
    VSDescription vector_scan_desc;
    vector_scan_desc.topk = topk;
    vector_scan_desc.column_name = function_col_name;

    if (query_column)
    {
        if (query_column->getNodeType() == QueryTreeNodeType::COLUMN)
        {
            const auto & query_column_typed = query_column->as<ColumnNode &>();
            vector_scan_desc.search_column_name = query_column_typed.getColumnName();

            DataTypePtr search_vector_column_type = query_column_typed.getColumnType();
            vector_scan_desc.vector_search_type = getSearchIndexDataType(search_vector_column_type);

            StorageMetadataPtr metadata_snapshot = nullptr;
            auto query_column_source = query_column_typed.getColumnSourceOrNull();
            if (query_column_source && query_column_source->getNodeType() == QueryTreeNodeType::TABLE)
            {
                const auto & table_storage = query_column_source->as<TableNode &>().getStorage();
                if (table_storage)
                    metadata_snapshot = table_storage->getInMemoryMetadataPtr();
            }

            String vector_scan_metric_type = getMetricType(metadata_snapshot, vector_scan_desc.vector_search_type, vector_scan_desc.search_column_name, context);

            /// Pass the correct direction to vector_scan_desc according to metric_type
            vector_scan_desc.direction = Poco::toUpper(vector_scan_metric_type) == "IP" ? -1 : 1;

            getAndCheckVectorScanInfoFromMetadata(metadata_snapshot, vector_scan_desc, context);
        }
        else
        {
            LOG_DEBUG(getLogger("commonMakeVectorScanDescription"), "query column node dump tree: {}", query_column->dumpTree());
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unexpected node type for query column: {}", query_column->getNodeType());
        }
    }

    if (query_vector)
    {
        const auto * constant_node = query_vector->as<ConstantNode>();
        if (constant_node)
        {
            /// Construct ColumnPtr from Constant Node
            vector_scan_desc.query_column = constant_node->getResultType()->createColumnConst(1, constant_node->getValue());
        }
        else if (const auto * get_scalar_function_node = query_vector->as<FunctionNode>();
                get_scalar_function_node && get_scalar_function_node->getFunctionName() == "__getScalar")
        {
            /// Allow constant folding through getScalar
            const auto * get_scalar_const_arg = get_scalar_function_node->getArguments().getNodes().at(0)->as<ConstantNode>();
            if (get_scalar_const_arg && context->hasQueryContext())
            {
                auto query_context = context->getQueryContext();
                auto scalar_string = toString(get_scalar_const_arg->getValue());
                if (query_context->hasScalar(scalar_string))
                {
                    auto scalar = query_context->getScalar(scalar_string);
                    vector_scan_desc.query_column = ColumnConst::create(scalar.getByPosition(0).column, 1);
                }
            }

            if(!vector_scan_desc.query_column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong query vector type {} in distance function", query_vector->getNodeType());
        }
        else
        {
            LOG_DEBUG(getLogger("commonMakeVectorScanDescription"), "query vector node dump tree: {}", query_vector->dumpTree());
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unexpected node type for query vector: {}", query_vector->getNodeType());
        }
    }

    LOG_DEBUG(getLogger("commonMakeVectorScanDescription"), "search column: {}", vector_scan_desc.search_column_name);

    return vector_scan_desc;
}

VSDescriptions extractVectorScanDescriptions(const QueryTreeNodes & vector_scan_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    VSDescriptions vector_scan_descriptions;

    for (size_t i = 0; i < vector_scan_func_nodes.size(); ++i)
    {
        const auto & vector_scan_func_node_typed = vector_scan_func_nodes[i]->as<FunctionNode &>();

        const auto & arguments_nodes = vector_scan_func_node_typed.getArguments().getNodes();

        auto vector_scan_desc = commonMakeVectorScanDescription(vector_scan_func_node_typed.getFunctionName(), arguments_nodes[0], arguments_nodes[1],
                                                                static_cast<int>(limit_length), context);

        const auto & parameters_nodes = vector_scan_func_node_typed.getParameters().getNodes();
        vector_scan_desc.parameters.reserve(parameters_nodes.size());

        for (const auto & parameter_node : parameters_nodes)
        {
            /// Function parameters constness validated during analysis stage
            vector_scan_desc.parameters.push_back(parameter_node->as<ConstantNode &>().getValue());
        }

        vector_scan_descriptions.push_back(vector_scan_desc);
    }

    return vector_scan_descriptions;
}

TextSearchInfoPtr commonMakeTextSearchInfo(
    const String & search_name,
    const String & function_col_name,
    QueryTreeNodePtr query_column,
    QueryTreeNodePtr query_text,
    int topk,
    const Array & parameters)
{
    String text_column_name;

    if (query_column)
    {
        if (query_column->getNodeType() == QueryTreeNodeType::COLUMN)
        {
            const auto & query_column_typed = query_column->as<ColumnNode &>();
            text_column_name = query_column_typed.getColumnName();

            StorageMetadataPtr metadata_snapshot = nullptr;
            bool is_remote_storage = false;

            auto query_column_source = query_column_typed.getColumnSourceOrNull();
            if (query_column_source && query_column_source->getNodeType() == QueryTreeNodeType::TABLE)
            {
                const auto & table_storage = query_column_source->as<TableNode &>().getStorage();
                if (table_storage)
                {
                    metadata_snapshot = table_storage->getInMemoryMetadataPtr();
                    is_remote_storage = table_storage->isRemote();
                }
            }

            if (!is_remote_storage)
                checkTantivyIndex(metadata_snapshot, text_column_name);
        }
    }

    String query_text_value;
    if (query_text)
    {
        if (query_text->getNodeType() == QueryTreeNodeType::CONSTANT)
        {
            query_text_value = query_text->as<ConstantNode &>().getValue().safeGet<String>();
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unexpected node type for query text: {}", query_text->getNodeType());
    }

    LOG_DEBUG(getLogger("makeTextSearchInfo"), "text search column: {}, query text: {}", text_column_name, query_text_value);

    bool enable_natural_language_query = true;
    String text_operator = "OR";

    for (const auto & arg : parameters)
    {
        String param_str = arg.safeGet<String>();
        auto pos = param_str.find('=');
        if (pos == std::string::npos || pos == 0 || pos == param_str.length())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The parameter {} inside {} function should be key-value format string, separated by `=`.", param_str, search_name);

        String param_key = param_str.substr(0, pos);
        String param_value = param_str.substr(pos + 1);

        if (param_key == "enable_nlq")
        {
            if (param_value.size() == 1)
            {
                /// 0 / 1
                std::stringstream param_ss(param_value);
                param_ss >> enable_natural_language_query;
                if (param_ss.fail())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "TextSearch parameter `enable_nlq` value should be bool");
            }
            else
            {
                /// boolalpha (true or false)
                std::stringstream param_ss_retry(param_value);
                param_ss_retry >> std::boolalpha >> enable_natural_language_query;
                if (param_ss_retry.fail() || !param_ss_retry.eof())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "TextSearch parameter `enable_nlq` value should be bool");
            }
        }
        else if (param_key == "operator")
        {
            if (param_value != "OR" && param_value != "AND")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "TextSearch parameter `operator` value should be either OR or AND");

            text_operator = param_value;
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown parameter {} for TextSearch", param_key);
    }

    return std::make_shared<TextSearchInfo>(text_column_name, query_text_value, function_col_name, topk, text_operator, enable_natural_language_query);
}

TextSearchInfoPtr makeTextSearchInfo(const QueryTreeNodes & text_search_func_nodes,
    const UInt64 & limit_length)
{
    if (text_search_func_nodes.size() != 1 || !text_search_func_nodes[0])
        return nullptr;

    const auto & text_search_func_node_typed = text_search_func_nodes[0]->as<FunctionNode &>();

    const auto & arguments_nodes = text_search_func_node_typed.getArguments().getNodes();

    const auto & parameters_nodes = text_search_func_node_typed.getParameters().getNodes();

    Array text_params(parameters_nodes.size());
    for (size_t i = 0; i < parameters_nodes.size(); ++i)
    {
        const auto & parameter_node = parameters_nodes[i];

        /// Function parameters constness validated during analysis stage
        const auto & constant_node = parameter_node->as<ConstantNode &>();
        if (constant_node.getResultType()->getTypeId() != TypeIndex::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters inside TextSearch function must be key-value format string, separated by `=`.");

        text_params[i] = constant_node.getValue().safeGet<String>();
    }

    auto tmp_text_search_info = commonMakeTextSearchInfo("TextSearch", text_search_func_node_typed.getFunctionName(), arguments_nodes[0],
                                                        arguments_nodes[1], static_cast<int>(limit_length), text_params);

    LOG_DEBUG(getLogger("makeTextSearchInfo"), "create text search function: {}", text_search_func_node_typed.getFunctionName());

    return tmp_text_search_info;
}

HybridSearchInfoPtr makeHybirdSearchInfo(const QueryTreeNodes & hybrid_search_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    if (hybrid_search_func_nodes.size() != 1 || !hybrid_search_func_nodes[0])
        return nullptr;

    const auto & hybrid_search_func_node_typed = hybrid_search_func_nodes[0]->as<FunctionNode &>();

    const auto & arguments_nodes = hybrid_search_func_node_typed.getArguments().getNodes();

    std::unordered_map<String, String> hybrid_parameters_map;
    std::vector<String> vector_scan_parameter;
    std::vector<String> text_search_parameters;

    const auto & parameters_nodes = hybrid_search_func_node_typed.getParameters().getNodes();
    for (const auto & parameter_node : parameters_nodes)
    {
        /// Function parameters constness validated during analysis stage
        const auto & constant_node = parameter_node->as<ConstantNode &>();
        if (constant_node.getResultType()->getTypeId() != TypeIndex::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All parameters inside HybridSearch function must be key-value format string, separated by `=`.");

        String param_str = constant_node.getValue().safeGet<String>();
        auto pos = param_str.find('=');
        if (pos == std::string::npos || pos == 0 || pos == param_str.length())
            throw Exception(ErrorCodes::ILLEGAL_HYBRID_SEARCH, "The parameter {} inside HybridSearch function should be key-value format string, separated by `=`.", param_str);

        String param_key = param_str.substr(0, pos);
        String param_value = param_str.substr(pos + 1);

        if (param_key == "fusion_type" || param_key == "fusion_weight" || param_key == "fusion_k" || param_key == "num_candidates")
        {
            if (hybrid_parameters_map.count(param_key) > 0)
            {
                throw Exception(ErrorCodes::ILLEGAL_HYBRID_SEARCH, "Multiple {} parameters in the HybridSearch function.", param_key);
            }
            hybrid_parameters_map[param_key] = param_value;
        }
        else if (param_key.find(vector_scan_parameter_prefix) == 0)
        {
            vector_scan_parameter.push_back(param_str.substr(std::strlen(vector_scan_parameter_prefix)));
        }
        else if (param_key == "enable_nlq" || param_key == "operator")
        {
            text_search_parameters.push_back(param_str);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_HYBRID_SEARCH, "Unknown parameter {} in the HybridSearch function.", param_key);
        }
    }

    /// Use num_candidates for vector scan's top-k to get more candidates results for hybrid search
    const auto & settings_ref = context->getSettingsRef();
    int num_candidates = 0;
    if (hybrid_parameters_map.contains("num_candidates"))
    {
        std::stringstream num_candidates_ss(hybrid_parameters_map["num_candidates"]);
        num_candidates_ss >> num_candidates;
        if (num_candidates_ss.fail() || !num_candidates_ss.eof())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HybridSearch parameter `num_candidates` value should be int");
    }

    /// Use default value (3 * topk) if specified num_candidates <= 0
    if (num_candidates <= 0)
        num_candidates = static_cast<int>(settings_ref.hybrid_search_top_k_multiple_base * limit_length);
    else if (static_cast<UInt64>(num_candidates) < limit_length)
    {
        /// num_candidates should be no less than limit N (top k)
        num_candidates = static_cast<int>(limit_length);
    }

    LOG_DEBUG(getLogger("makeHybirdSearchInfo"), "num_candidates is {}", num_candidates);

    /// make VSDescription for HybridSearchInfo
    auto vector_scan_desc = commonMakeVectorScanDescription("distance_func", arguments_nodes[0], arguments_nodes[2], num_candidates, context);

    /// Save vector_scan_parameter to vector_scan_desc's parameters
    if (!vector_scan_parameter.empty())
    {
        Array params_array(vector_scan_parameter.size());
        for (size_t i = 0; i < vector_scan_parameter.size(); ++i)
            params_array[i] = vector_scan_parameter[i];

        vector_scan_desc.parameters = params_array;
    }

    VSDescriptions vector_scan_descriptions;
    vector_scan_descriptions.push_back(vector_scan_desc);

    /// make TextSearchInfo for HybridSearchInfo
    Array text_params(text_search_parameters.size());
    for (size_t i = 0; i < text_search_parameters.size(); ++i)
        text_params[i] = text_search_parameters[i];

    auto tmp_text_search_info = commonMakeTextSearchInfo("HybridSearch", "textsearch_func", arguments_nodes[1], arguments_nodes[3], num_candidates, text_params);

    String hybrid_fusion_type = hybrid_parameters_map["fusion_type"];
    String function_column_name = hybrid_search_func_node_typed.getFunctionName();

    HybridSearchInfoPtr hybrid_search_info = nullptr;
    if (isRelativeScoreFusion(hybrid_fusion_type))
    {
        float hybrid_fusion_weight = static_cast<float>(settings_ref.hybrid_search_fusion_weight);
        if (hybrid_parameters_map.count("fusion_weight") > 0)
        {
            std::stringstream fusion_weight_ss(hybrid_parameters_map["fusion_weight"]);
            fusion_weight_ss >> hybrid_fusion_weight;
            if (fusion_weight_ss.fail())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HybridSearch parameter `fusion_weight` value should be float");
        }

        if (hybrid_fusion_weight < 0 || hybrid_fusion_weight > 1)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Wrong HybridSearch parameter for Relative Score Fusion(RSF), valid value is in interval [0.0f, 1.0f]");
        }

        hybrid_search_info = std::make_shared<HybridSearchInfo>(
            std::make_shared<VectorScanInfo>(vector_scan_descriptions),
            tmp_text_search_info,
            function_column_name, static_cast<int>(limit_length), hybrid_fusion_type, hybrid_fusion_weight);
    }
    else if (isRankFusion(hybrid_fusion_type))
    {
        int hybrid_fusion_k = static_cast<int>(settings_ref.hybrid_search_fusion_k);
        if (hybrid_parameters_map.count("fusion_k") > 0)
        {
            std::stringstream fusion_k_ss(hybrid_parameters_map["fusion_k"]);
            fusion_k_ss >> hybrid_fusion_k;
            if (fusion_k_ss.fail())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "HybridSearch parameter `fusion_k` value should be int");
        }

        if (hybrid_fusion_k < 0)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Wrong HybridSearch parameter for Reciprocal Rank Fusion(RRF), `fusion_k` is less than 0");
        }
        hybrid_search_info = std::make_shared<HybridSearchInfo>(
            std::make_shared<VectorScanInfo>(vector_scan_descriptions),
            tmp_text_search_info,
            function_column_name, static_cast<int>(limit_length), hybrid_fusion_type, hybrid_fusion_k);
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Wrong or missing HybridSearch function parameter `fusion_type`. Valid values: 'RSF' and 'RRF'");
    }

    return hybrid_search_info;
}

std::optional<SpecialSearchAnalysisResult> analyzeVectorScan(const QueryTreeNodes & vector_scan_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    auto vector_scan_descs = extractVectorScanDescriptions(vector_scan_func_nodes, context, limit_length);

    if (vector_scan_descs.empty())
        return std::nullopt;

    SpecialSearchAnalysisResult vector_scan_analysis_result;
    vector_scan_analysis_result.has_vector_scan = true;
    vector_scan_analysis_result.vector_scan_descriptions = std::move(vector_scan_descs);
    return vector_scan_analysis_result;
}

std::optional<SpecialSearchAnalysisResult> analyzeTextSearch(const QueryTreeNodes & text_search_func_nodes,
    const UInt64 & limit_length)
{
    auto text_search_info = makeTextSearchInfo(text_search_func_nodes, limit_length);

    if (!text_search_info)
        return std::nullopt;

    SpecialSearchAnalysisResult text_search_analysis_result;
    text_search_analysis_result.has_text_search = true;
    text_search_analysis_result.text_search_info = std::move(text_search_info);
    return text_search_analysis_result;
}

std::optional<SpecialSearchAnalysisResult> analyzeHybridSearch(const QueryTreeNodes & hybrid_search_func_nodes,
    const ContextPtr & context,
    const UInt64 & limit_length)
{
    auto hybrid_search_info = makeHybirdSearchInfo(hybrid_search_func_nodes, context, limit_length);

    if (!hybrid_search_info)
        return std::nullopt;

    SpecialSearchAnalysisResult hybrid_search_analysis_result;
    hybrid_search_analysis_result.has_hybrid_search = true;
    hybrid_search_analysis_result.hybrid_search_info = std::move(hybrid_search_info);
    return hybrid_search_analysis_result;
}

/** Construct special search analysis result if query tree has distance, textsearch or hybridsearch functions.
  * Actions before special search are added into actions chain, if result is not null optional.
  */
std::optional<SpecialSearchAnalysisResult> analyzeSpecialSearch(const QueryTreeNodePtr & query_tree,
    const ContextPtr & context)
{
    LOG_DEBUG(getLogger("analyzeSpecialSearch"), "analyzeSpecialSearch");

    auto & query_node = query_tree->as<QueryNode &>();
    auto special_search_function_nodes = collectHybridSearchFunctionNodes(query_tree);

    if (special_search_function_nodes.size() == 0)
        return std::nullopt;

    /// Get topK from limit N
    UInt64 limit_length = 0;

    if (query_node.hasLimit())
    {
        /// Constness of limit is validated during query analysis stage
        limit_length = query_node.getLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();
    }

    LOG_DEBUG(getLogger("analyzeSpecialSearch"), "limit_length={}", limit_length);

    /// Check the function name to find which search: vector scan, text or hybrid search
    const auto & search_func_node = special_search_function_nodes[0]->as<FunctionNode &>();
    String func_name = search_func_node.getFunctionName();

    LOG_DEBUG(getLogger("analyzeSpecialSearch"), "search func node name={}", func_name);

    std::optional<SpecialSearchAnalysisResult> special_search_analysis_result_optional = std::nullopt;

    if (isVectorScanFunc(func_name))
        special_search_analysis_result_optional = analyzeVectorScan(special_search_function_nodes, context, limit_length);
    else if (isTextSearch(func_name))
        special_search_analysis_result_optional = analyzeTextSearch(special_search_function_nodes, limit_length);
    else if (isHybridSearch(func_name))
        special_search_analysis_result_optional = analyzeHybridSearch(special_search_function_nodes, context, limit_length);

    /// Add source column of vector column or text column to analysis result
    if (special_search_analysis_result_optional)
    {
        /// Find column source of the vector column a in search function
        const auto & arguments_nodes = search_func_node.getArguments().getNodes();
        const auto & vector_column = arguments_nodes[0];
        if (vector_column && vector_column->as<ColumnNode>())
        {
            auto vector_column_node = vector_column->as<ColumnNode>();
            auto vector_column_source_node = vector_column_node->getColumnSource();
            auto column_source_node_type = vector_column_source_node->getNodeType();

            if (column_source_node_type != QueryTreeNodeType::TABLE &&
                column_source_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
                column_source_node_type != QueryTreeNodeType::QUERY &&
                column_source_node_type != QueryTreeNodeType::UNION &&
                column_source_node_type != QueryTreeNodeType::ARRAY_JOIN)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected table, table function, array join, query or union column source. Actual {}",
                    vector_column_source_node->formatASTForErrorMessage());

            special_search_analysis_result_optional->source_weak_pointer = vector_column_source_node;
        }
    }

    return special_search_analysis_result_optional;
}

}
