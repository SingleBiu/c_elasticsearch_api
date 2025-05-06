/*
 * @Author: SingleBiu
 * @Date: 2025-05-06 20:31:04
 * @LastEditors: SingleBiu
 * @LastEditTime: 2025-05-07 00:48:26
 * @Description: 使用json作为输入的Elasticsearch的C语言API
 * 编译使用 gcc es_main.c cJSON.c -o main
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "cJSON.h"

// 读取文件的函数
char* read_file(const char* filename) {
    FILE* file = fopen(filename, "r");
    if (file == NULL) {
        perror("Failed to open file");
        return NULL;
    }

    fseek(file, 0, SEEK_END);
    long length = ftell(file);
    fseek(file, 0, SEEK_SET);

    char* data = (char*)malloc(length + 1);
    if (data == NULL) {
        perror("Failed to allocate memory");
        fclose(file);
        return NULL;
    }

    fread(data, 1, length, file);
    data[length] = '\0'; // 确保字符串以null结尾
    fclose(file);

    return data;
}

int main(int argc ,char *argvp[])
{
    // 文件指针
    FILE *fp = NULL;
    // CJSON对象初始化
    cJSON *cjson_obj = cJSON_CreateObject();
    // ID
    char char_arr_id[16];
    // name
    char char_arr_name[24];
    // json的指针
    char *jp = NULL;
    //获取输入
    printf("Please intput ID(max length 13):\n");
    scanf("%s",&char_arr_id);
    printf("Please input name(max length 23):\n");
    scanf("%s",&char_arr_name);
    // 向CJSON中添加对象
    cJSON_AddItemToObject(cjson_obj,"ID",cJSON_CreateString(char_arr_id));
    cJSON_AddItemToObject(cjson_obj,"name",cJSON_CreateString(char_arr_name));
    // 打印CJSON对象
    jp = cJSON_Print(cjson_obj);
    printf("Json data:\n%s\n",jp);
    system("pause");
    // 创建并打开文件
    fp = fopen("./insert.json","w+");
    // 写入文件
    fprintf(fp,jp);
    // 关闭文件
    fclose(fp);
    // 释放内存
    free(jp);
    cJSON_Delete(cjson_obj);

    // 测试Elasticsearch 
    // (需要将elasticsearch-9.0.0/config/elasticsearch.yml中的xpack.security.enabled设置成false)
    system("curl -X GET \"http://127.0.0.1:9200?pretty\"");
    system("pause");

    //删除索引
    system("curl -X DELETE \"http://127.0.0.1:9200/index?pretty\" -H Content-Type:application/json");
    system("pause");

    //创建索引
    system("curl -X PUT \"http://127.0.0.1:9200/index?pretty\" -H Content-Type:application/json -d @index.json");
    system("pause");
    
    // 添加id为1的数据
    system("curl -X POST \"http://127.0.0.1:9200/index/_doc/1?pretty\" -H Content-Type:application/json -d @insert.json");
    system("pause");
    
    // 添加数据 文档ID自动生成
    system("curl -X POST \"http://127.0.0.1:9200/index/_doc?pretty\" -H Content-Type:application/json -d @insert2.json");
    system("pause");

    // 查询所有数据
    system("curl -X POST \"http://127.0.0.1:9200/index/_search?pretty\" -H Content-Type:application/json -d @query_all.json");
    system("pause");

    // 按字段查询数据
    system("curl -X POST \"http://127.0.0.1:9200/index/_search?pretty\" -H Content-Type:application/json -d @query.json");
    system("pause");

    // 查询数据 另一种写法
    system("curl -X POST \"http://127.0.0.1:9200/index/_search?q=A1\" -H Content-Type:application/json");
    system("pause");

    // 修改数据 根据update.json的内容将id为1的文档的内容ID修改为B2
    system("curl -X POST \"http://127.0.0.1:9200/index/_update/1?pretty\" -H Content-Type:application/json -d @update.json");
    system("pause");

    // DEBUG 获取所有结果 发现id为1的内容已经修改
    system("curl -X POST \"http://127.0.0.1:9200/index/_search?pretty\" -H Content-Type:application/json -d @query_all.json");
    system("pause");

    // 删除数据 (删除了name为Fox3的数据)
    system("curl -X POST \"http://127.0.0.1:9200/index/_delete_by_query?pretty\" -H Content-Type:application/json -d @delete.json");
    system("pause");

    // DEBUG 获取所有结果 发现name为Fox3的数据不存在了
    system("curl -X POST \"http://127.0.0.1:9200/index/_search?pretty\" -H Content-Type:application/json -d @query_all.json");
    system("pause");

    // 查询数据 将内容存入文件
    system("curl -X POST \"http://127.0.0.1:9200/index/_search?pretty\" -H Content-Type:application/json -d @query2.json > result1_query.json");
    system("pause");

    const char* filename = "result1_query.json";
    char* json_data = read_file(filename);
    if (json_data == NULL) {
        printf("Error read json file\n");
        system("pause");
        return 1;
    }

    // 解析JSON字符串
    cJSON* root = cJSON_Parse(json_data);
    if (root == NULL) {
        const char* error_ptr = cJSON_GetErrorPtr();
        if (error_ptr != NULL) {
            fprintf(stderr, "Error before: %s\n", error_ptr);
        }
        free(json_data);
        printf("Error parse json file\n");
        system("pause");
        return 1;
    }

    // 提取hits数组
    cJSON* hits = cJSON_GetObjectItem(root, "hits");
    cJSON* hits2 = cJSON_GetObjectItem(hits,"hits");
    if (cJSON_IsArray(hits2) && cJSON_GetArraySize(hits2) > 0) {
        // 提取hits数组中的第一个元素
        cJSON* hit = cJSON_GetArrayItem(hits2, 0);
        if (hit != NULL) {
            // 提取_source对象
            cJSON* _source = cJSON_GetObjectItem(hit, "_source");
            if (cJSON_IsObject(_source)) {
                // 提取name字段
                cJSON* name = cJSON_GetObjectItem(_source, "name");
                if (name != NULL && cJSON_IsString(name)) {
                    printf("Name: %s\n", name->valuestring);
                } else {
                    printf("Name field not found or not a string.\n");
                }
            } else {
                printf("_source field is not an object.\n");
            }
        } else {
            printf("No hits found in the response.\n");
        }
    } else {
        printf("Hits field is not an array or is empty.\n");
    }

    // 释放内存
    cJSON_Delete(root);
    free(json_data);


    system("pause");
    return 0;
}