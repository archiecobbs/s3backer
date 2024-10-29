
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 *
 * Copyright 2008-2023 Archie L. Cobbs <archie.cobbs@gmail.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of portions of this program with the
 * OpenSSL library under certain conditions as described in each
 * individual source file, and distribute linked combinations including
 * the two.
 *
 * You must obey the GNU General Public License in all respects for all
 * of the code used other than OpenSSL. If you modify file(s) with this
 * exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do
 * so, delete this exception statement from your version. If you delete
 * this exception statement from all source files in the program, then
 * also delete it here.
 */

#include "s3backer.h"
#include "http_io.h"
#include "compress.h"
#include "util.h"

// HTTP definitions
#define HTTP_GET                    "GET"
#define HTTP_PUT                    "PUT"
#define HTTP_DELETE                 "DELETE"
#define HTTP_HEAD                   "HEAD"
#define HTTP_POST                   "POST"
#define HTTP_MOVED_PERMANENTLY      301
#define HTTP_FOUND                  302
#define HTTP_NOT_MODIFIED           304
#define HTTP_TEMPORARY_REDIRECT     307
#define HTTP_PERMANENT_REDIRECT     308
#define HTTP_UNAUTHORIZED           401
#define HTTP_FORBIDDEN              403
#define HTTP_NOT_FOUND              404
#define HTTP_PRECONDITION_FAILED    412
#define AUTH_HEADER                 "Authorization"
#define CTYPE_HEADER                "Content-Type"
#define CONTENT_ENCODING_HEADER     "Content-Encoding"
#define ACCEPT_ENCODING_HEADER      "Accept-Encoding"
#define ETAG_HEADER                 "ETag"
#define CONTENT_ENCODING_ENCRYPT    "encrypt"
#define MD5_HEADER                  "Content-MD5"
#define ACL_HEADER                  "x-amz-acl"
#define CONTENT_SHA256_HEADER       "x-amz-content-sha256"
#define SSE_HEADER                  "x-amz-server-side-encryption"
#define SSE_KEY_ID_HEADER           "x-amz-server-side-encryption-aws-kms-key-id"
#define STORAGE_CLASS_HEADER        "x-amz-storage-class"
#define FILE_SIZE_HEADER            "x-amz-meta-s3backer-filesize"
#define BLOCK_SIZE_HEADER           "x-amz-meta-s3backer-blocksize"
#define MOUNT_TOKEN_HEADER          "x-amz-meta-s3backer-mount-token"
#define HMAC_HEADER                 "x-amz-meta-s3backer-hmac"
#define IF_MATCH_HEADER             "If-Match"
#define IF_NONE_MATCH_HEADER        "If-None-Match"

// Minumum HTTP status code we consider an error
#define HTTP_STATUS_ERROR_MINIMUM   300                     // we don't support redirects

// MIME type for blocks
#define CONTENT_TYPE                "application/x-s3backer-block"

// Mount token file
#define MOUNT_TOKEN_FILE            "s3backer-mounted"
#define MOUNT_TOKEN_FILE_MIME_TYPE  "text/plain"

// HTTP `Date' and `x-amz-date' header formats
#define HTTP_DATE_HEADER            "Date"
#define AWS_DATE_HEADER             "x-amz-date"
#define HTTP_DATE_BUF_FMT           "%a, %d %b %Y %H:%M:%S GMT"
#define AWS_DATE_BUF_FMT            "%Y%m%dT%H%M%SZ"
#define DATE_BUF_SIZE               64

// Upper bound on the length of an URL representing one block
#define URL_BUF_SIZE(config)        (strlen((config)->baseURL)                  \
                                      + strlen((config)->bucket) + 1            \
                                      + strlen((config)->prefix)                \
                                      + S3B_BLOCK_NUM_DIGITS                    \
                                      + strlen(BLOCK_HASH_PREFIX_SEPARATOR)     \
                                      + S3B_BLOCK_NUM_DIGITS + 2)

// Separator string used when "--blockHashPrefix" is in effect
#define BLOCK_HASH_PREFIX_SEPARATOR "-"

// Bucket listing API constants
#define LIST_PARAM_MARKER           "marker"
#define LIST_PARAM_PREFIX           "prefix"
#define LIST_PARAM_MAX_KEYS         "max-keys"

#define LIST_ELEM_LIST_BUCKET_RESLT "ListBucketResult"
#define LIST_ELEM_IS_TRUNCATED      "IsTruncated"
#define LIST_ELEM_CONTENTS          "Contents"
#define LIST_ELEM_KEY               "Key"
#define LIST_TRUE                   "true"

// Bulk Delete API constants
#define DELETE_ELEM_DELETE          "Delete"
#define DELETE_ELEM_OBJECT          "Object"
#define DELETE_ELEM_KEY             "Key"
#define DELETE_ELEM_DELETE_RESULT   "DeleteResult"
#define DELETE_ELEM_ERROR           "Error"
#define DELETE_ELEM_CODE            "Code"
#define DELETE_ELEM_MESSAGE         "Message"

// How many blocks to list or delete at a time
#define LIST_BLOCKS_CHUNK           1000
#define DELETE_BLOCKS_CHUNK         1000

// Maximum payload size in bytes to capture for debug
#define MAX_DEBUG_PAYLOAD_SIZE      100000

// PBKDF2 key generation iterations
#define PBKDF2_ITERATIONS           5000

// Enable to debug encryption key stuff
#define DEBUG_ENCRYPTION            0

// Enable to debug authentication stuff
#define DEBUG_AUTHENTICATION        0

// Enable to debug parsing block list response
#define DEBUG_BLOCK_LIST            0

// Version 4 authentication stuff
#define SIGNATURE_ALGORITHM         "AWS4-HMAC-SHA256"
#define ACCESS_KEY_PREFIX           "AWS4"
#define S3_SERVICE_NAME             "s3"
#define SIGNATURE_TERMINATOR        "aws4_request"
#define SECURITY_TOKEN_HEADER       "x-amz-security-token"

// EC2 IAM stuff
#define EC2_IAM_TOKEN_REQ_URL           "http://169.254.169.254/latest/api/token"
#define EC2_IAM_TOKEN_REQ_TTL_HEADER    "X-aws-ec2-metadata-token-ttl-seconds"
#define EC2_IAM_TOKEN_REQ_TTL_SECONDS   21600
#define EC2_IAM_META_DATA_URLBASE       "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
#define EC2_IAM_META_DATA_TOKEN_HEADER  "X-aws-ec2-metadata-token"

// EC2 IAM JSON response fields
#define EC2_IAM_META_DATA_ACCESSID  "AccessKeyId"
#define EC2_IAM_META_DATA_ACCESSKEY "SecretAccessKey"
#define EC2_IAM_META_DATA_TOKEN     "Token"

// TCP keep-alive
#define TCP_KEEP_ALIVE_IDLE         200
#define TCP_KEEP_ALIVE_INTERVAL     60

// Misc
#define WHITESPACE                  " \t\v\f\r\n"
#if MD5_DIGEST_LENGTH != 16
#error unexpected MD5_DIGEST_LENGTH
#endif
#if SHA_DIGEST_LENGTH != 20
#error unexpected MD5_DIGEST_LENGTH
#endif

/*
 * HTTP-based implementation of s3backer_store.
 *
 * This implementation does no caching or consistency checking.
 */

// Internal definitions
struct curl_holder {
    CURL                        *curl;
    LIST_ENTRY(curl_holder)     link;
};

// Block survey per-thread info
struct http_io_survey {
    struct http_io_private      *priv;
    pthread_t                   thread;
    s3b_block_t                 min_name;
    s3b_block_t                 max_name;
    block_list_func_t           *callback;
    void                        *callback_arg;
};

// Internal state
struct http_io_private {
    struct http_io_conf         *config;
    struct http_io_stats        stats;
    LIST_HEAD(, curl_holder)    curls;
    pthread_mutex_t             mutex;
    bitmap_t                    *non_zero;                      // config->nonzero_bitmap is moved to here
    pthread_t                   iam_thread;                     // IAM credentials refresh thread
    u_char                      iam_thread_alive;               // IAM thread was successfully created
    u_char                      iam_thread_shutdown;            // Flag to the IAM thread telling it to exit

    // Block survey info
    struct http_io_survey       *survey_threads;                // survey threads that are running now, if any
    u_int                       num_survey_threads;             // the number of survey threads that are still alive
    u_int                       num_survey_threads_joinable;    // the number of survey threads not yet joined
    pthread_cond_t              survey_done;                    // indicates last survey thread has finished
    volatile int                abort_survey;                   // set to 1 to abort block survey
    int                         survey_error;                   // error from any survey thread

    // Encryption info
    const EVP_CIPHER            *cipher;
    u_int                       keylen;                         // length of key and ivkey
    u_char                      key[EVP_MAX_KEY_LENGTH];        // key used to encrypt data
    u_char                      ivkey[EVP_MAX_KEY_LENGTH];      // key used to encrypt block number to get IV for data
    struct hmac_engine          *hmac;
};

// I/O buffers
struct http_io_bufs {
    size_t      rdremain;       // number of bytes left in download buffer
    size_t      wrremain;       // number of bytes left in upload buffer
    char        *rddata;        // download buffer
    const char  *wrdata;        // upload buffer
};

// I/O state when reading/writing a block
struct http_io {

    // I/O buffers
    struct http_io_bufs bufs;

    // Configuration
    struct http_io_conf *config;                // configuration

    // XML parser info
    XML_Parser          xml;                    // XML parser
    int                 xml_error;              // XML parse error (if any)
    int                 xml_error_line;         // XML parse error line
    int                 xml_error_column;       // XML parse error column
    char                *xml_path;              // Current XML path
    char                *xml_text;              // Current XML text
    int                 handler_error;          // error encountered during parsing

    // Bucket object listing info
    int                 list_truncated;         // returned list was truncated
    char                *start_after;           // where to start next round of listing
    s3b_block_t         min_name;               // inclusive lower bound for block name/prefix
    s3b_block_t         max_name;               // inclusive upper bound for block name/prefix
    s3b_block_t         *block_list;            // the blocks we found this iteration
    u_int               num_blocks;             // number of blocks in "block_list"

    // Bulk delete info
    char                *bulk_delete_err_key;
    char                *bulk_delete_err_code;
    char                *bulk_delete_err_msg;

    // Other info that needs to be passed around
    CURL                *curl;                  // back reference to CURL instance
    const char          *method;                // HTTP method
    const char          *url;                   // HTTP URL
    struct curl_slist   *headers;               // HTTP headers
    const char          *sse;                   // Server Side Encryption
    void                *dest;                  // Block data (when reading)
    const void          *src;                   // Block data (when writing)
    s3b_block_t         block_num;              // The block we're reading/writing
    u_int               buf_size;               // Size of data buffer (dest if reading, src if writing)
    u_int               *content_lengthp;       // Returned Content-Length
    uintmax_t           file_size;              // file size from "x-amz-meta-s3backer-filesize"
    u_int               block_size;             // block size from "x-amz-meta-s3backer-blocksize"
    int32_t             mount_token;            // mount_token from "x-amz-meta-s3backer-mount-token"
    u_int               expect_304;             // a verify request; expect a 304 response
    u_char              etag[MD5_DIGEST_LENGTH];// parsed ETag header (must look like an MD5 hash)
    u_char              hmac[SHA_DIGEST_LENGTH];// parsed "x-amz-meta-s3backer-hmac" header
    char                content_encoding[32];   // received content encoding
    check_cancel_t      *check_cancel;          // write check-for-cancel callback
    void                *check_cancel_arg;      // write check-for-cancel callback argument

    // Info used to capture error response payloads
    long                http_status;            // response status, if known, else zero
    char                *error_payload;         // payload of error response
    size_t              error_payload_len;      // error response length
};

// CURL prepper function type - returns 1 on succes, 0 on error
typedef int http_io_curl_prepper_t(struct http_io_private *const priv, CURL *curl, struct http_io *io);

// s3backer_store functions
static int http_io_create_threads(struct s3backer_store *s3b);
static int http_io_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);
static int http_io_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value);
static int http_io_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
  u_char *actual_etag, const u_char *expect_etag, int strict);
static int http_io_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *etag,
  check_cancel_t *check_cancel, void *check_cancel_arg);
static int http_io_flush_blocks(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks, long timeout);
static int http_io_bulk_zero(struct s3backer_store *const s3b, const s3b_block_t *block_nums, u_int num_blocks);
static int http_io_survey_non_zero(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);
static int http_io_shutdown(struct s3backer_store *s3b);
static void http_io_destroy(struct s3backer_store *s3b);

// Other functions
static http_io_curl_prepper_t http_io_head_prepper;
static http_io_curl_prepper_t http_io_read_prepper;
static http_io_curl_prepper_t http_io_write_prepper;
static http_io_curl_prepper_t http_io_xml_prepper;
static http_io_curl_prepper_t http_io_iamcreds_token_prepper;
static http_io_curl_prepper_t http_io_iamcreds_prepper;

// S3 REST API functions
static void http_io_get_block_url(char *buf, size_t bufsiz, struct http_io_conf *config, s3b_block_t block_num);
static void http_io_get_mount_token_file_url(char *buf, size_t bufsiz, struct http_io_conf *config);
static int http_io_add_auth(struct http_io_private *priv, struct http_io *io, time_t now, const void *payload, size_t plen);
static int http_io_add_auth2(struct http_io_private *priv, struct http_io *io, time_t now, const void *payload, size_t plen);
static int http_io_add_auth4(struct http_io_private *priv, struct http_io *io, time_t now, const void *payload, size_t plen);
static size_t url_encode(const char *src, size_t len, char *dst, int buflen, int encode_slash);
static void digest_url_encoded(EVP_MD_CTX* hash_ctx, const char *data, size_t len, int encode_slash);
static char *canonicalize_query_string(const char *query, size_t len);

// EC2 IAM thread
static void *update_iam_credentials_main(void *arg);
static int update_iam_credentials(struct http_io_private *priv);
static int update_iam_credentials_with_token(struct http_io_private *const priv, const char *token);
static char *parse_json_field(struct http_io_private *priv, const char *json, const char *field);

// Block survey functions
static int http_io_list_blocks_range(struct http_io_private *priv,
    s3b_block_t min, s3b_block_t max, block_list_func_t *callback, void *arg);
static void *http_io_list_blocks_worker_main(void *arg);
static void http_io_list_blocks_elem_end(void *arg, const XML_Char *name);
static block_list_func_t http_io_list_blocks_callback;
static void http_io_wait_for_survey_threads_to_exit(struct http_io_private *const priv);

// Bulk delete
static void http_io_bulk_delete_elem_end(void *arg, const XML_Char *name);

// XML query functions
static int http_io_xml_io_init(struct http_io_private *const priv, struct http_io *io, const char *method, char *url);
static int http_io_xml_io_exec(struct http_io_private *const priv, struct http_io *io,
    void (*end_handler)(void *arg, const XML_Char *name));
static void http_io_xml_io_destroy(struct http_io_private *const priv, struct http_io *io);
static size_t http_io_curl_xml_reader(const void *ptr, size_t size, size_t nmemb, void *stream);
static void http_io_xml_elem_start(void *arg, const XML_Char *name, const XML_Char **atts);
static void http_io_xml_text(void *arg, const XML_Char *s, int len);

// HTTP and curl functions
static int http_io_perform_io(struct http_io_private *priv, struct http_io *io, http_io_curl_prepper_t *prepper);
static size_t http_io_curl_reader(const void *ptr, size_t size, size_t nmemb, void *stream);
static size_t http_io_curl_writer(void *ptr, size_t size, size_t nmemb, void *stream);
static size_t http_io_curl_header(void *ptr, size_t size, size_t nmemb, void *stream);
static int http_io_add_header(struct http_io_private *priv, struct http_io *io, const char *fmt, ...)
    __attribute__ ((__format__ (__printf__, 3, 4)));
static int http_io_add_data_and_auth_headers(struct http_io_private *priv, struct http_io *io);
static CURL *http_io_acquire_curl(struct http_io_private *priv, struct http_io *io);
static int http_io_safe_to_cache_curl_handle(CURLcode curl_code, long http_code);
static void http_io_release_curl(struct http_io_private *priv, CURL **curlp, int may_cache);
static int http_io_reader_error_check(struct http_io *const io, const void *ptr, size_t len);
static void http_io_free_error_payload(struct http_io *const io);
static void http_io_log_error_payload(struct http_io *const io);
static int http_io_sockopt_callback(void *clientp, curl_socket_t curlfd, curlsocktype purpose);

// Misc
static void http_io_openssl_locker(int mode, int i, const char *file, int line);
static u_long http_io_openssl_ider(void);
static void http_io_base64_encode(char *buf, size_t bufsiz, const void *data, size_t len);
static u_int http_io_crypt(struct http_io_private *priv,
    s3b_block_t block_num, int enc, const u_char *src, u_int len, u_char *dst, u_int dmax);
static void http_io_authsig(struct http_io_private *priv, s3b_block_t block_num, const u_char *src, u_int len, u_char *hmac);
static void update_hmac_from_header(struct hmac_ctx *ctx, struct http_io *io,
  const char *name, int value_only, char *sigbuf, size_t sigbuflen);
static s3b_block_t http_io_block_hash_prefix(s3b_block_t block_num);
static int http_io_parse_hex(const char *str, u_char *buf, u_int nbytes);
static int http_io_parse_hex_block_num(const char *string, s3b_block_t *block_nump);
static void http_io_prhex(char *buf, const u_char *data, size_t len);
static int http_io_header_name_sort(const void *ptr1, const void *ptr2);
static int http_io_parse_header(struct http_io *io, const char *input,
    const char *header, int num_conversions, const char *fmt, ...);
static void http_io_init_io(struct http_io_private *priv, struct http_io *io, const char *method, const char *url);
static void http_io_curl_header_reset(struct http_io *const io);
static int http_io_verify_etag_provided(struct http_io *io);
static int http_io_curl_setopt_long(struct http_io_private *priv, CURL *curl, CURLoption option, long value);
static int http_io_curl_setopt_ptr(struct http_io_private *priv, CURL *curl, CURLoption option, const void *ptr);
static int http_io_curl_setopt_off(struct http_io_private *priv, CURL *curl, CURLoption option, curl_off_t offset);

// Internal variables
static pthread_mutex_t *openssl_locks;
static int num_openssl_locks;
static u_char zero_etag[MD5_DIGEST_LENGTH];
static u_char zero_hmac[SHA_DIGEST_LENGTH];

/*
 * Constructor
 *
 * On error, returns NULL and sets `errno'.
 */
struct s3backer_store *
http_io_create(struct http_io_conf *config)
{
    struct s3backer_store *s3b;
    struct http_io_private *priv;
    struct curl_holder *holder;
    int nlocks;
    int r;

    // Sanity check: we can really only handle one instance
    if (openssl_locks != NULL) {
        (*config->log)(LOG_ERR, "http_io_create() called twice");
        r = EALREADY;
        goto fail0;
    }

    // Initialize structures
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        goto fail0;
    }
    s3b->create_threads = http_io_create_threads;
    s3b->meta_data = http_io_meta_data;
    s3b->set_mount_token = http_io_set_mount_token;
    s3b->read_block = http_io_read_block;
    s3b->write_block = http_io_write_block;
    s3b->bulk_zero = http_io_bulk_zero;
    s3b->flush_blocks = http_io_flush_blocks;
    s3b->survey_non_zero = http_io_survey_non_zero;
    s3b->shutdown = http_io_shutdown;
    s3b->destroy = http_io_destroy;
    if ((priv = calloc(1, sizeof(*priv))) == NULL) {
        r = errno;
        goto fail1;
    }
    priv->config = config;
    if ((r = pthread_mutex_init(&priv->mutex, NULL)) != 0)
        goto fail2;
    if ((r = pthread_cond_init(&priv->survey_done, NULL)) != 0) {
        goto fail3;
    }
    LIST_INIT(&priv->curls);
    s3b->data = priv;

    // Initialize openssl
    num_openssl_locks = CRYPTO_num_locks();
    if ((openssl_locks = malloc(num_openssl_locks * sizeof(*openssl_locks))) == NULL) {
        r = errno;
        goto fail4;
    }
    for (nlocks = 0; nlocks < num_openssl_locks; nlocks++) {
        if ((r = pthread_mutex_init(&openssl_locks[nlocks], NULL)) != 0)
            goto fail5;
    }
    CRYPTO_set_locking_callback(http_io_openssl_locker);
    CRYPTO_set_id_callback(http_io_openssl_ider);
    if ((priv->hmac = hmac_engine_create()) == NULL) {
        r = errno;
        goto fail5;
    }

    // Avoid GCC unused-function warnings
    (void)http_io_openssl_locker;
    (void)http_io_openssl_ider;

    // Initialize encryption
    if (config->encryption != NULL) {
        char saltbuf[strlen(config->bucket) + 1 + strlen(config->prefix) + 1];
        u_int cipher_key_len;
        u_int cipher_block_size;
        u_int cipher_iv_length;

        // Sanity checks
        assert(config->password != NULL);
        assert(config->block_size % EVP_MAX_IV_LENGTH == 0);

        // Find encryption algorithm
        OpenSSL_add_all_ciphers();
        if ((priv->cipher = EVP_get_cipherbyname(config->encryption)) == NULL) {
            (*config->log)(LOG_ERR, "unknown encryption cipher `%s'", config->encryption);
            r = EINVAL;
            goto fail6;
        }
        cipher_key_len = EVP_CIPHER_key_length(priv->cipher);
        priv->keylen = config->key_length > 0 ? config->key_length : cipher_key_len;
        if (priv->keylen < cipher_key_len || priv->keylen > sizeof(priv->key)) {
            (*config->log)(LOG_ERR, "key length %u for cipher `%s' is out of range", priv->keylen, config->encryption);
            r = EINVAL;
            goto fail6;
        }

        // Sanity check cipher is a block cipher
        cipher_block_size = EVP_CIPHER_block_size(priv->cipher);
        cipher_iv_length = EVP_CIPHER_iv_length(priv->cipher);
        if (cipher_block_size <= 1 || cipher_block_size != cipher_iv_length) {
            (*config->log)(LOG_ERR, "invalid cipher `%s' (block size %u, IV length %u); only block ciphers are supported",
              config->encryption, cipher_block_size, cipher_iv_length);
            r = EINVAL;
            goto fail6;
        }

        // Hash password to get bulk data encryption key
        snvprintf(saltbuf, sizeof(saltbuf), "%s/%s", config->bucket, config->prefix);
        if ((r = PKCS5_PBKDF2_HMAC_SHA1(config->password, strlen(config->password),
          (u_char *)saltbuf, strlen(saltbuf), PBKDF2_ITERATIONS, priv->keylen, priv->key)) != 1) {
            (*config->log)(LOG_ERR, "failed to create encryption key");
            r = EINVAL;
            goto fail6;
        }

        // Hash the bulk encryption key to get the IV encryption key
        if ((r = PKCS5_PBKDF2_HMAC_SHA1((char *)priv->key, priv->keylen,
          priv->key, priv->keylen, PBKDF2_ITERATIONS, priv->keylen, priv->ivkey)) != 1) {
            (*config->log)(LOG_ERR, "failed to create encryption key");
            r = EINVAL;
            goto fail6;
        }

        // Encryption debug
#if DEBUG_ENCRYPTION
    {
        char keybuf[priv->keylen * 2 + 1];
        char ivkeybuf[priv->keylen * 2 + 1];
        http_io_prhex(keybuf, priv->key, priv->keylen);
        http_io_prhex(ivkeybuf, priv->ivkey, priv->keylen);
        (*config->log)(LOG_DEBUG, "ENCRYPTION INIT: cipher=\"%s\" pass=\"%s\" salt=\"%s\" key=0x%s ivkey=0x%s", config->encryption, config->password, saltbuf, keybuf, ivkeybuf);
    }
#endif
    }

    // Initialize cURL
    curl_global_init(CURL_GLOBAL_ALL);

    // Initialize IAM credentials
    if (config->ec2iam_role != NULL && (r = update_iam_credentials(priv)) != 0)
        goto fail7;

    // Take ownership of non-zero block bitmap
    priv->non_zero = config->nonzero_bitmap;
    config->nonzero_bitmap = NULL;

    // Done
    return s3b;

fail7:
    while ((holder = LIST_FIRST(&priv->curls)) != NULL) {
        curl_easy_cleanup(holder->curl);
        LIST_REMOVE(holder, link);
        free(holder);
    }
    curl_global_cleanup();
fail6:
    hmac_engine_free(priv->hmac);
fail5:
    CRYPTO_set_locking_callback(NULL);
    CRYPTO_set_id_callback(NULL);
    while (nlocks > 0)
        pthread_mutex_destroy(&openssl_locks[--nlocks]);
    free(openssl_locks);
    openssl_locks = NULL;
    num_openssl_locks = 0;
fail4:
    pthread_cond_destroy(&priv->survey_done);
fail3:
    pthread_mutex_destroy(&priv->mutex);
fail2:
    free(priv);
fail1:
    free(s3b);
fail0:
    (*config->log)(LOG_ERR, "http_io creation failed: %s", strerror(r));
    errno = r;
    return NULL;
}

/*
 * Destructor
 */
static void
http_io_destroy(struct s3backer_store *const s3b)
{
    struct http_io_private *const priv = s3b->data;
    struct curl_holder *holder;

    // Clean up openssl
    while (num_openssl_locks > 0)
        pthread_mutex_destroy(&openssl_locks[--num_openssl_locks]);
    free(openssl_locks);
    openssl_locks = NULL;
    CRYPTO_set_locking_callback(NULL);
    CRYPTO_set_id_callback(NULL);

    // Clean up cURL
    hmac_engine_free(priv->hmac);
    while ((holder = LIST_FIRST(&priv->curls)) != NULL) {
        curl_easy_cleanup(holder->curl);
        LIST_REMOVE(holder, link);
        free(holder);
    }
    curl_global_cleanup();

    // Free structures
    pthread_cond_destroy(&priv->survey_done);
    pthread_mutex_destroy(&priv->mutex);
    bitmap_free(&priv->non_zero);
    free(priv);
    free(s3b);
}

static int
http_io_shutdown(struct s3backer_store *const s3b)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    int r;

    // Signal survey threads to stop
    priv->abort_survey = 1;

    // Lock mutex
    pthread_mutex_lock(&priv->mutex);

    // Shut down IAM thread, if any
    if (priv->iam_thread_alive) {

        // Make IAM thread stop
        priv->iam_thread_shutdown = 1;
        if ((r = pthread_cancel(priv->iam_thread)) != 0)
            (*config->log)(LOG_ERR, "pthread_cancel: %s", strerror(r));
        priv->iam_thread_alive = 0;

        // Reap IAM thread
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        (*config->log)(LOG_DEBUG, "waiting for EC2 IAM thread to shutdown");
        if ((r = pthread_join(priv->iam_thread, NULL)) != 0)
            (*config->log)(LOG_ERR, "pthread_join: %s", strerror(r));
        else
            (*config->log)(LOG_DEBUG, "EC2 IAM thread successfully shutdown");
        pthread_mutex_lock(&priv->mutex);
    }

    // Wait for block survey threads to exit, if any
    http_io_wait_for_survey_threads_to_exit(priv);

    // Unlock mutex
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Done
    return 0;
}

void
http_io_get_stats(struct s3backer_store *s3b, struct http_io_stats *stats)
{
    struct http_io_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memcpy(stats, &priv->stats, sizeof(*stats));
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
}

void
http_io_clear_stats(struct s3backer_store *s3b)
{
    struct http_io_private *const priv = s3b->data;

    pthread_mutex_lock(&priv->mutex);
    memset(&priv->stats, 0, sizeof(priv->stats));
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
}

static int
http_io_flush_blocks(struct s3backer_store *s3b, const s3b_block_t *block_nums, u_int num_blocks, long timeout)
{
    return 0;                           // we are stateless, so there's nothing to flush
}

static int
http_io_survey_non_zero(struct s3backer_store *s3b, block_list_func_t *callback, void *arg)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    const int max_threads = config->list_blocks_threads;
    s3b_block_t last_possible_name;
    int r = 0;

    // Determine the last possible block name we need to scan for
    last_possible_name = config->blockHashPrefix ? ~(s3b_block_t)0 : config->num_blocks - 1;

    // Lock mutex
    pthread_mutex_lock(&priv->mutex);
    assert(priv->num_survey_threads == 0);
    assert(priv->num_survey_threads_joinable == 0);

    // Allocate survey_threads array
    if (priv->num_survey_threads != 0) {
        r = EINPROGRESS;
        goto done;
    }
    if ((priv->survey_threads = calloc(max_threads, sizeof(*priv->survey_threads))) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc: %s", strerror(r));
        goto done;
    }

    // Initialize per-thread infos and start threads
    priv->survey_error = 0;
    while (priv->num_survey_threads < max_threads) {
        const int thread_index = priv->num_survey_threads;
        struct http_io_survey *const survey = &priv->survey_threads[thread_index];

        // Configure this thread
        survey->priv = priv;
        survey->callback = callback;
        survey->callback_arg = arg;

        // Configure this thread's portion of the range, but being careful to handle weird corner cases
        survey->min_name = thread_index > 0 ? survey[-1].max_name + 1 : (s3b_block_t)0;
        if (survey->min_name > last_possible_name)
            survey->min_name = last_possible_name;
        if (thread_index == max_threads - 1)
            survey->max_name = last_possible_name;
        else {
            survey->max_name = ((off_t)last_possible_name * (thread_index + 1)) / max_threads;
            if (survey->max_name < survey->min_name)
                survey->max_name = survey->min_name;
            if (survey->max_name > last_possible_name)
                survey->max_name = last_possible_name;
        }

        // Start this thread
        if ((r = pthread_create(&survey->thread, NULL, http_io_list_blocks_worker_main, survey)) != 0) {
            (*config->log)(LOG_ERR, "pthread_create: %s", strerror(r));
            priv->abort_survey = 1;
            break;
        }
        priv->num_survey_threads++;
    }
    priv->num_survey_threads_joinable = priv->num_survey_threads;

    // Wait for survey threads to finish
    http_io_wait_for_survey_threads_to_exit(priv);
    if (r == 0)
        r = priv->survey_error;

done:
    // Done
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    return r;
}

// This assumes the mutex is locked
static void
http_io_wait_for_survey_threads_to_exit(struct http_io_private *const priv)
{
    struct http_io_conf *const config = priv->config;
    int r;

    // Wait for all survey threads to finish
    while (priv->num_survey_threads > 0)
        pthread_cond_wait(&priv->survey_done, &priv->mutex);

    // Join them
    while (priv->num_survey_threads_joinable > 0) {
        if ((r = pthread_join(priv->survey_threads[--priv->num_survey_threads_joinable].thread, NULL)) != 0)
            (*config->log)(LOG_ERR, "pthread_join: %s", strerror(r));
    }

    // Reset state
    free(priv->survey_threads);
    priv->survey_threads = NULL;
}

static void *
http_io_list_blocks_worker_main(void *arg)
{
    struct http_io_survey *const info = arg;
    struct http_io_private *const priv = info->priv;
    int r = 0;

    // Scan my range (if non-empty)
    if (info->min_name <= info->max_name)
        r = http_io_list_blocks_range(priv, info->min_name, info->max_name, http_io_list_blocks_callback, info);

    // Finish up
    pthread_mutex_lock(&priv->mutex);
    if (priv->survey_error == 0)
        priv->survey_error = r;
    if (priv->num_survey_threads > 0 && --priv->num_survey_threads == 0)
        pthread_cond_broadcast(&priv->survey_done);
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    return NULL;
}

static int
http_io_list_blocks_callback(void *arg, const s3b_block_t *block_nums, u_int num_blocks)
{
    struct http_io_survey *const info = arg;

    if (info->priv->abort_survey)
        return ECANCELED;
    return (*info->callback)(info->callback_arg, block_nums, num_blocks);
}

//
// Scan blocks in the range "min" (inclusive) to "max" (inclusive).
//
// Note "min" and "max" refer to the first S3B_BLOCK_NUM_DIGITS characters of the block's S3 object name (after any "--prefix"),
// not necessarily the block number; these are different things when "--blockHashPrefix" is used, otherwise they are the same.
//
static int
http_io_list_blocks_range(struct http_io_private *priv, s3b_block_t min, s3b_block_t max, block_list_func_t *callback, void *arg)
{
    struct http_io_conf *const config = priv->config;
    char last_possible_path[strlen(config->prefix) + S3B_BLOCK_NUM_DIGITS + 1];
    s3b_block_t block_list[LIST_BLOCKS_CHUNK];
    struct http_io io;
    int r;

    // Initialize XML query
    if ((r = http_io_xml_io_init(priv, &io, HTTP_GET, NULL)) != 0)              // "url" is set below
        return r;
    io.block_list = block_list;
    io.min_name = min;
    io.max_name = max;

    // Determine the last possible valid block name (or block hash prefix) we want to search for
    snvprintf(last_possible_path, sizeof(last_possible_path), "%s%0*jx", config->prefix, S3B_BLOCK_NUM_DIGITS, (uintmax_t)max);

    // Initialize "io.start_after", which says where to continue listing names each time around the loop
    if (min == (s3b_block_t)0)
        r = asprintf(&io.start_after, "%s%0*jx%c", config->prefix, S3B_BLOCK_NUM_DIGITS - 1, (uintmax_t)0, '0' - 1);
    else
        r = asprintf(&io.start_after, "%s%0*jx", config->prefix, S3B_BLOCK_NUM_DIGITS, (uintmax_t)(min - 1));
    if (r == -1) {
        r = errno;
        (*config->log)(LOG_ERR, "asprintf: %s", strerror(r));
        io.start_after = NULL;
        goto done;
    }

    // List blocks
    while (1) {
        char start_after_urlencoded[3 * strlen(io.start_after) + 1];
        char urlbuf[strlen(config->baseURL)
          + strlen(config->bucket)
          + sizeof("?" LIST_PARAM_MARKER "=") + sizeof(start_after_urlencoded)
          + sizeof("&" LIST_PARAM_MAX_KEYS "=") + 16];

        // Format URL (note: URL parameters must be in "canonical query string" format for proper authentication)
        url_encode(io.start_after, strlen(io.start_after), start_after_urlencoded, sizeof(start_after_urlencoded), 1);
        snvprintf(urlbuf, sizeof(urlbuf), "%s%s?%s=%s&%s=%u", config->baseURL, config->vhost ? "" : config->bucket,
          LIST_PARAM_MARKER, start_after_urlencoded, LIST_PARAM_MAX_KEYS, LIST_BLOCKS_CHUNK);
        io.url = urlbuf;

        // Perform operation
        assert(io.num_blocks == 0);
#if DEBUG_BLOCK_LIST
        (*config->log)(LOG_DEBUG, "list: querying range [%0*jx, %0*jx] starting after \"%s\"",
            S3B_BLOCK_NUM_DIGITS, (uintmax_t)min, S3B_BLOCK_NUM_DIGITS, (uintmax_t)max, io.start_after);
#endif
        if ((r = http_io_xml_io_exec(priv, &io, http_io_list_blocks_elem_end)) != 0)
            break;

        // Invoke callback with the blocks we found
        if (io.num_blocks > 0) {
            if ((r = (*callback)(arg, block_list, io.num_blocks)) != 0)
                break;
            io.num_blocks = 0;
        }

        // Are we done?
        if (!io.list_truncated || strcmp(io.start_after, last_possible_path) >= 0)
            break;
    }

done:
    // Done
    http_io_xml_io_destroy(priv, &io);
    free(io.start_after);
    return r;
}

static int
http_io_xml_prepper(struct http_io_private *const priv, CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    if (http_io_add_data_and_auth_headers(priv, io) != 0)
        return 0;
    if (io->src != NULL) {
        io->bufs.wrdata = io->src;
        io->bufs.wrremain = io->buf_size;
        if (!http_io_curl_setopt_ptr(priv, curl, CURLOPT_READFUNCTION, http_io_curl_writer)
          || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_READDATA, io)
          || !http_io_curl_setopt_long(priv, curl, CURLOPT_UPLOAD, 1)
          || !http_io_curl_setopt_long(priv, curl, CURLOPT_POST, 1)
          || !http_io_curl_setopt_off(priv, curl, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)io->buf_size))
            return 0;
    }
    if (!http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEFUNCTION, http_io_curl_xml_reader)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEDATA, io)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HTTPHEADER, io->headers)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_ACCEPT_ENCODING, "")
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_HTTP_CONTENT_DECODING, 1))
        return 0;
    http_io_curl_header_reset(io);
    return 1;
}

static size_t
http_io_curl_xml_reader(const void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct http_io *const io = (struct http_io *)stream;
    size_t total = size * nmemb;

    // Check for error payload
    if (http_io_reader_error_check(io, ptr, total))
        return total;

    // If we are in an error state, just bail
    if (io->xml_error != XML_ERROR_NONE || io->handler_error != 0)
        return total;

    // Run new payload bytes through XML parser
    if (XML_Parse(io->xml, ptr, total, 0) != XML_STATUS_OK) {
        io->xml_error = XML_GetErrorCode(io->xml);
        io->xml_error_line = XML_GetCurrentLineNumber(io->xml);
        io->xml_error_column = XML_GetCurrentColumnNumber(io->xml);
    }

    // Done
    return total;
}

static void
http_io_xml_elem_start(void *arg, const XML_Char *name, const XML_Char **atts)
{
    struct http_io *const io = (struct http_io *)arg;
    const size_t plen = strlen(io->xml_path);
    char *newbuf;

    // If we are in an error state, just bail
    if (io->xml_error != XML_ERROR_NONE || io->handler_error != 0)
        return;

    // Update current path
    if ((newbuf = realloc(io->xml_path, plen + 1 + strlen(name) + 1)) == NULL) {
        io->handler_error = errno;
        (*io->config->log)(LOG_ERR, "realloc: %s", strerror(errno));
        return;
    }
    io->xml_path = newbuf;
    io->xml_path[plen] = '/';
    strcpy(io->xml_path + plen + 1, name);

    // Reset text buffer
    io->xml_text[0] = '\0';

#if DEBUG_BLOCK_LIST
    // Debug
    (*io->config->log)(LOG_DEBUG, "list: new path: \"%s\"", io->xml_path);
#endif
}

static void
http_io_list_blocks_elem_end(void *arg, const XML_Char *name)
{
    struct http_io *const io = (struct http_io *)arg;
    struct http_io_conf *const config = io->config;

    // If we are in an error state, just bail
    if (io->xml_error != XML_ERROR_NONE || io->handler_error != 0)
        return;

    // Handle <Truncated> tag
    if (strcmp(io->xml_path, "/" LIST_ELEM_LIST_BUCKET_RESLT "/" LIST_ELEM_IS_TRUNCATED) == 0) {
        io->list_truncated = strcmp(io->xml_text, LIST_TRUE) == 0;
#if DEBUG_BLOCK_LIST
        (*config->log)(LOG_DEBUG, "list: parsed truncated=%d", io->list_truncated);
#endif
        goto done;
    }

    // Handle <Key> tag
    if (strcmp(io->xml_path, "/" LIST_ELEM_LIST_BUCKET_RESLT "/" LIST_ELEM_CONTENTS "/" LIST_ELEM_KEY) == 0) {
        s3b_block_t hash_value;
        s3b_block_t block_num;
        char *new_start_after;

#if DEBUG_BLOCK_LIST
        (*config->log)(LOG_DEBUG, "list: key=\"%s\"", io->xml_text);
#endif

        // Attempt to parse key as a block's object name and add to list if successful
        if (http_io_parse_block(config->prefix, config->num_blocks,
          config->blockHashPrefix, io->xml_text, &hash_value, &block_num) == 0) {
#if DEBUG_BLOCK_LIST
            (*config->log)(LOG_DEBUG, "list: parsed key=\"%s\" -> hash=%0*jx block=%0*jx",
              io->xml_text, S3B_BLOCK_NUM_DIGITS, (uintmax_t)hash_value, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
#endif
            if (hash_value >= io->min_name && hash_value <= io->max_name) {             // avoid out-of-range callbacks
                if (io->num_blocks < LIST_BLOCKS_CHUNK)
                    io->block_list[io->num_blocks++] = block_num;
                else {
                    (*config->log)(LOG_ERR, "list: rec'd more than %d blocks?", LIST_BLOCKS_CHUNK);
                    io->handler_error = EINVAL;
                }
            }
        }
#if DEBUG_BLOCK_LIST
        else
            (*config->log)(LOG_DEBUG, "list: can't parse key=\"%s\"", io->xml_text);
#endif

        // Save the last name we see as the starting point for next time
        if ((new_start_after = realloc(io->start_after, strlen(io->xml_text) + 1)) == NULL) {
            io->handler_error = errno;
            (*config->log)(LOG_ERR, "strdup: %s", strerror(errno));
        } else {
            io->start_after = new_start_after;
            strcpy(io->start_after, io->xml_text);
        }
    }

done:
    // Update current XML path
    assert(strrchr(io->xml_path, '/') != NULL);
    *strrchr(io->xml_path, '/') = '\0';

    // Reset text buffer
    io->xml_text[0] = '\0';
}

static void
http_io_xml_text(void *arg, const XML_Char *s, int len)
{
    struct http_io *const io = (struct http_io *)arg;
    struct http_io_conf *const config = io->config;
    size_t current_len;
    char *new_buf;

    // If we are in an error state, just bail
    if (io->xml_error != XML_ERROR_NONE || io->handler_error != 0)
        return;

    // Extend text buffer
    current_len = strlen(io->xml_text);
    if ((new_buf = realloc(io->xml_text, current_len + len + 1)) == NULL) {
        io->handler_error = errno;
        (*config->log)(LOG_ERR, "realloc(%u + %d): %s", (u_int)current_len, len + 1, strerror(errno));
        return;
    }
    io->xml_text = new_buf;

    // Append text to buffer
    memcpy(io->xml_text + current_len, s, len);
    io->xml_text[current_len + len] = '\0';
}

/*
 * Parse a block's item name (including prefix and block hash prefix if any) and returns the result in *block_nump.
 *
 * Also returns the hash value, if any, in *hash_valuep; if not using block hash prefix then *hash_valuep = *block_nump.
 */
int
http_io_parse_block(const char *prefix, s3b_block_t num_blocks, int blockHashPrefix,
    const char *name, s3b_block_t *hash_valuep, s3b_block_t *block_nump)
{
    const size_t plen = strlen(prefix);
    s3b_block_t hash_value = 0;
    s3b_block_t block_num = 0;

    // Parse prefix
    if (strncmp(name, prefix, plen) != 0)
        return -1;
    name += plen;

    // Parse block hash prefix followed by BLOCK_HASH_PREFIX_SEPARATOR (if so configured)
    if (blockHashPrefix) {
        if (http_io_parse_hex_block_num(name, &hash_value) == -1)
            return -1;
        name += S3B_BLOCK_NUM_DIGITS;
        if (strncmp(name, BLOCK_HASH_PREFIX_SEPARATOR, strlen(BLOCK_HASH_PREFIX_SEPARATOR)) != 0)
            return -1;
        name += strlen(BLOCK_HASH_PREFIX_SEPARATOR);
    }

    // Parse block number
    if (http_io_parse_hex_block_num(name, &block_num) == -1)
        return -1;
    name += S3B_BLOCK_NUM_DIGITS;
    if (*name != '\0' || block_num >= num_blocks)
        return -1;

    // Verify hash matches what's expected
    if (!blockHashPrefix)
        hash_value = block_num;
    else if (hash_value != http_io_block_hash_prefix(block_num))
        return -1;

    // Done
    *hash_valuep = hash_value;
    *block_nump = block_num;
    return 0;
}

/*
 * Parse a hexadecimal block number value, which should be S3B_BLOCK_NUM_DIGITS lowercase digits.
 *
 * Returns zero on success, -1 on failure.
 */
static int
http_io_parse_hex_block_num(const char *string, s3b_block_t *valuep)
{
    s3b_block_t value = 0;
    int i;

    // Parse block number
    for (i = 0; i < S3B_BLOCK_NUM_DIGITS; i++) {
        const char ch = string[i];

        value <<= 4;
        if (ch >= '0' && ch <= '9')
            value |= ch - '0';
        else if (ch >= 'a' && ch <= 'f')
            value |= ch - 'a' + 10;
        else
            return -1;
    }

    // Done
    *valuep = value;
    return 0;
}

/*
 * Append deterministic hash value based on block number for even name distribution.
 *
 * Ref: https://github.com/archiecobbs/s3backer/issues/80
 * Ref: https://crypto.stackexchange.com/questions/16219/cryptographic-hash-function-for-32-bit-length-input-keys
 */
void
http_io_format_block_hash(int blockHashPrefix, char *buf, size_t bufsiz, s3b_block_t block_num)
{
    assert(bufsiz >= S3B_BLOCK_NUM_DIGITS + strlen(BLOCK_HASH_PREFIX_SEPARATOR) + 1);
    if (blockHashPrefix) {
        snvprintf(buf, bufsiz, "%0*jx%s",
          S3B_BLOCK_NUM_DIGITS, (uintmax_t)http_io_block_hash_prefix(block_num), BLOCK_HASH_PREFIX_SEPARATOR);
    } else
        *buf = '\0';
}

/*
 * Calculate deterministic hash value based on block number for even name distribution.
 *
 * Ref: https://github.com/archiecobbs/s3backer/issues/80
 * Ref: https://crypto.stackexchange.com/questions/16219/cryptographic-hash-function-for-32-bit-length-input-keys
 */
s3b_block_t
http_io_block_hash_prefix(s3b_block_t block_num)
{
    s3b_block_t hash;
    int n;

    hash = block_num;
    for (n = 12; n > 0; n--)
        hash = ((hash >> 8) ^ hash) * 0x6b + n;
    return hash;
}

static int
http_io_create_threads(struct s3backer_store *s3b)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    int r = 0;

    // Start IAM updater thread if appropriate
    pthread_mutex_lock(&priv->mutex);
    if (config->ec2iam_role != NULL) {
        assert(!priv->iam_thread_alive);
        if ((r = pthread_create(&priv->iam_thread, NULL, update_iam_credentials_main, priv)) == 0)
            priv->iam_thread_alive = 1;
        else
            (*config->log)(LOG_ERR, "failed to create IAM updater thread: %s", strerror(r));
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Done
    return r;
}

static int
http_io_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char urlbuf[URL_BUF_SIZE(config)];
    struct http_io io;
    int r;

    // Initialize I/O info
    http_io_init_io(priv, &io, HTTP_HEAD, urlbuf);

    // Construct URL for the first block
    http_io_get_block_url(urlbuf, sizeof(urlbuf), config, 0);

    // Perform operation
    if ((r = http_io_perform_io(priv, &io, http_io_head_prepper)) != 0)
        goto done;

    // Extract filesystem sizing information
    if (io.file_size == 0 || io.block_size == 0) {
        r = ENOENT;
        goto done;
    }
    *file_sizep = (off_t)io.file_size;
    *block_sizep = io.block_size;

done:
    //  Clean up
    curl_slist_free_all(io.headers);
    return r;
}

static int
http_io_head_prepper(struct http_io_private *const priv, CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    if (http_io_add_data_and_auth_headers(priv, io) != 0)
        return 0;
    if (!http_io_curl_setopt_long(priv, curl, CURLOPT_NOBODY, 1)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEFUNCTION, http_io_curl_reader)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEDATA, io)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HEADERFUNCTION, http_io_curl_header)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HEADERDATA, io)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HTTPHEADER, io->headers))
        return 0;
    http_io_curl_header_reset(io);
    return 1;
}

static int
http_io_set_mount_token(struct s3backer_store *s3b, int32_t *old_valuep, int32_t new_value)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char urlbuf[URL_BUF_SIZE(config) + sizeof(MOUNT_TOKEN_FILE)];
    const time_t now = time(NULL);
    struct http_io io;
    int r = 0;

    // Initialize I/O info
    http_io_init_io(priv, &io, HTTP_HEAD, urlbuf);

    // Construct URL for the mount token file
    http_io_get_mount_token_file_url(urlbuf, sizeof(urlbuf), config);

    // Get old value
    if (old_valuep != NULL) {

        // See if object exists
        switch ((r = http_io_perform_io(priv, &io, http_io_head_prepper))) {
        case ENOENT:
            *old_valuep = 0;
            r = 0;
            break;
        case 0:
            *old_valuep = io.mount_token;
            if (*old_valuep == 0)                       // backward compatibility
                *old_valuep = 1;
            break;
        default:
            goto done;
        }
    }

    // Set new value
    if (new_value >= 0) {
        char content[_POSIX_HOST_NAME_MAX + DATE_BUF_SIZE + 32];
        u_char md5[MD5_DIGEST_LENGTH];
        char md5buf[MD5_DIGEST_LENGTH * 2 + 1];

        // Reset I/O info
        curl_slist_free_all(io.headers);
        http_io_init_io(priv, &io, new_value != 0 ? HTTP_PUT : HTTP_DELETE, urlbuf);

        // To set the flag PUT some content containing current date
        if (new_value != 0) {
            struct tm tm;

            // Create content for the mount token file (timestamp)
            gethostname(content, sizeof(content) - 1);
            content[sizeof(content) - 1] = '\0';
            strftime(content + strlen(content), sizeof(content) - strlen(content), "\n" AWS_DATE_BUF_FMT "\n", gmtime_r(&now, &tm));
            io.src = content;
            io.buf_size = strlen(content);

            // Add Content-Type header
            http_io_add_header(priv, &io, "%s: %s", CTYPE_HEADER, MOUNT_TOKEN_FILE_MIME_TYPE);

            // Add Content-MD5 header
            md5_quick(content, strlen(content), md5);
            http_io_base64_encode(md5buf, sizeof(md5buf), md5, MD5_DIGEST_LENGTH);
            http_io_add_header(priv, &io, "%s: %s", MD5_HEADER, md5buf);

            // Add Mount-Token header
            http_io_add_header(priv, &io, "%s: %08x", MOUNT_TOKEN_HEADER, (int)new_value);

            // Add ACL header
            http_io_add_header(priv, &io, "%s: %s", ACL_HEADER, config->accessType);
        }

        // Add Server Side Encryption header(s) (if needed)
        if (config->sse != NULL && new_value != 0) {
            http_io_add_header(priv, &io, "%s: %s", SSE_HEADER, config->sse);
            if (strcmp(config->sse, SSE_AWS_KMS) == 0)
                http_io_add_header(priv, &io, "%s: %s", SSE_KEY_ID_HEADER, config->sse_key_id);
        }

        // Add storage class header (if needed)
        if (config->storage_class != NULL)
            http_io_add_header(priv, &io, "%s: %s", STORAGE_CLASS_HEADER, config->storage_class);

        // Perform operation to set or clear mount token
        r = http_io_perform_io(priv, &io, http_io_write_prepper);
    }

done:
    //  Clean up
    curl_slist_free_all(io.headers);
    return r;
}

static int
update_iam_credentials(struct http_io_private *const priv)
{
    struct http_io_conf *const config = priv->config;

    // If using IMDSv2, get an auth token first
    if (config->ec2iam_imdsv2) {
        struct http_io io;
        char tokenbuf[1024];
        size_t buflen;
        int r;

        // Initialize I/O info
        http_io_init_io(priv, &io, HTTP_PUT, EC2_IAM_TOKEN_REQ_URL);
        io.dest = tokenbuf;
        io.buf_size = sizeof(tokenbuf);

        // Add TTL header
        http_io_add_header(priv, &io, "%s: %d", EC2_IAM_TOKEN_REQ_TTL_HEADER, EC2_IAM_TOKEN_REQ_TTL_SECONDS);

        // Perform operation
        (*config->log)(LOG_INFO, "acquiring EC2 IAM IMDSv2 auth token from %s", io.url);
        if ((r = http_io_perform_io(priv, &io, http_io_iamcreds_token_prepper)) != 0) {
            (*config->log)(LOG_ERR, "failed to acquire EC2 IAM IMDSv2 auth token from %s: %s", io.url, strerror(r));
            return r;
        }

        // Determine how many bytes we read and NUL-terminate result
        buflen = io.buf_size - io.bufs.rdremain;
        if (buflen > sizeof(tokenbuf) - 1)
            buflen = sizeof(tokenbuf) - 1;
        tokenbuf[buflen] = '\0';

#if DEBUG_AUTHENTICATION
        (*config->log)(LOG_DEBUG, "ec2iam_imdsv2: got auth token \"%s\"", tokenbuf);
#endif

        // Proceed with token (IMDSv2)
        return update_iam_credentials_with_token(priv, tokenbuf);
    } else

    // Proceed witout token (IMDSv1)
    return update_iam_credentials_with_token(priv, NULL);
}

static int
update_iam_credentials_with_token(struct http_io_private *const priv, const char *token)
{
    struct http_io_conf *const config = priv->config;
    char *urlbuf;
    struct http_io io;
    char buf[2048] = { '\0' };
    char *access_id = NULL;
    char *access_key = NULL;
    char *iam_token = NULL;
    size_t buflen;
    int r;

    // Build URL
    if (asprintf(&urlbuf, "%s%s", EC2_IAM_META_DATA_URLBASE, config->ec2iam_role) == -1) {
        r = errno;
        (*config->log)(LOG_ERR, "%s: asprintf() failed: %s", "update_iam_credentials", strerror(r));
        return r;
    }

    // Initialize I/O info
    http_io_init_io(priv, &io, HTTP_GET, urlbuf);
    io.dest = buf;
    io.buf_size = sizeof(buf);

    // Add token header, if any
    if (token != NULL)
        http_io_add_header(priv, &io, "%s: %s", EC2_IAM_META_DATA_TOKEN_HEADER, token);

    // Perform operation
    (*config->log)(LOG_INFO, "acquiring EC2 IAM credentials from %s", io.url);
    if ((r = http_io_perform_io(priv, &io, http_io_iamcreds_prepper)) != 0) {
        switch (r) {
        case ENOENT:
            (*config->log)(LOG_ERR, "EC2 IAM credentials \"%s\" not found", config->ec2iam_role);
            break;
        default:
            (*config->log)(LOG_ERR, "failed to acquire EC2 IAM credentials from %s: %s", io.url, strerror(r));
            break;
        }
        free(urlbuf);
        return r;
    }

    // Determine how many bytes we read
    buflen = io.buf_size - io.bufs.rdremain;
    if (buflen > sizeof(buf) - 1)
        buflen = sizeof(buf) - 1;
    buf[buflen] = '\0';

    // Find credentials in JSON response
    if ((access_id = parse_json_field(priv, buf, EC2_IAM_META_DATA_ACCESSID)) == NULL
      || (access_key = parse_json_field(priv, buf, EC2_IAM_META_DATA_ACCESSKEY)) == NULL
      || (iam_token = parse_json_field(priv, buf, EC2_IAM_META_DATA_TOKEN)) == NULL) {
        (*config->log)(LOG_ERR, "failed to extract EC2 IAM credentials from response: %s", strerror(errno));
        free(access_id);
        free(access_key);
        free(urlbuf);
        return EINVAL;
    }

    // Update credentials
    pthread_mutex_lock(&priv->mutex);
    free(config->accessId);
    free(config->accessKey);
    free(config->iam_token);
    config->accessId = access_id;
    config->accessKey = access_key;
    config->iam_token = iam_token;
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    (*config->log)(LOG_INFO, "successfully updated EC2 IAM credentials from %s", io.url);
    free(urlbuf);

    // Done
    return 0;
}

static int
http_io_iamcreds_token_prepper(struct http_io_private *const priv, CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    io->bufs.rdremain = io->buf_size;
    io->bufs.rddata = io->dest;
    io->bufs.wrremain = 0;                  // We do a "PUT" but we don't actually send anything
    io->bufs.wrdata = NULL;
    if (!http_io_curl_setopt_ptr(priv, curl, CURLOPT_READFUNCTION, http_io_curl_writer)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_READDATA, io)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEFUNCTION, http_io_curl_reader)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEDATA, io)
      || !http_io_curl_setopt_off(priv, curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)io->buf_size)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_CUSTOMREQUEST, io->method)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HTTPHEADER, io->headers)
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_UPLOAD, 1)
      || !http_io_curl_setopt_off(priv, curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)0))
        return 0;
    return 1;
}

static void *
update_iam_credentials_main(void *arg)
{
    struct http_io_private *const priv = arg;

    while (!priv->iam_thread_shutdown) {

        // Sleep for five minutes, or until woken up by pthread_cancel()
        sleep(300);

        // Shutting down?
        if (priv->iam_thread_shutdown)
            break;

        // Attempt to update credentials
        update_iam_credentials(priv);
    }

    // Done
    return NULL;
}

static char *
parse_json_field(struct http_io_private *priv, const char *json, const char *field)
{
    struct http_io_conf *const config = priv->config;
    regmatch_t match[2];
    regex_t regex;
    char buf[128];
    char *value;
    size_t vlen;
    int r;

    snvprintf(buf, sizeof(buf), "\"%s\"[[:space:]]*:[[:space:]]*\"([^\"]+)\"", field);
    memset(&regex, 0, sizeof(regex));
    if ((r = regcomp(&regex, buf, REG_EXTENDED)) != 0) {
        regerror(r, &regex, buf, sizeof(buf));
        (*config->log)(LOG_INFO, "regex compilation failed: %s", buf);
        errno = EINVAL;
        return NULL;
    }
    if ((r = regexec(&regex, json, sizeof(match) / sizeof(*match), match, 0)) != 0) {
        regerror(r, &regex, buf, sizeof(buf));
        (*config->log)(LOG_INFO, "failed to find JSON field \"%s\" in credentials response: %s", field, buf);
        regfree(&regex);
        errno = EINVAL;
        return NULL;
    }
    regfree(&regex);
    vlen = match[1].rm_eo - match[1].rm_so;
    if ((value = malloc(vlen + 1)) == NULL) {
        r = errno;
        (*config->log)(LOG_INFO, "malloc: %s", strerror(r));
        errno = r;
        return NULL;
    }
    memcpy(value, json + match[1].rm_so, vlen);
    value[vlen] = '\0';
    return value;
}

static int
http_io_iamcreds_prepper(struct http_io_private *const priv, CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    io->bufs.rdremain = io->buf_size;
    io->bufs.rddata = io->dest;
    if (!http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEFUNCTION, http_io_curl_reader)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEDATA, io)
      || !http_io_curl_setopt_off(priv, curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)io->buf_size)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HTTPHEADER, io->headers)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_ACCEPT_ENCODING, "")
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_HTTP_CONTENT_DECODING, 0))
        return 0;
    return 1;
}

static int
http_io_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_etag, const u_char *expect_etag, int strict)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char urlbuf[URL_BUF_SIZE(config)];
    char accept_encoding[128];
    int encrypted = 0;
    struct http_io io;
    u_int did_read;
    char *layer;
    int i;
    int r;

    // Sanity check
    if (config->block_size == 0 || block_num >= config->num_blocks)
        return EINVAL;

    // Read zero blocks when bitmap indicates empty until non-zero content is written
    if (priv->non_zero != NULL) {
        pthread_mutex_lock(&priv->mutex);
        if (!bitmap_test(priv->non_zero, block_num)) {
            priv->stats.empty_blocks_read++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            memset(dest, 0, config->block_size);
            if (actual_etag != NULL)
                memset(actual_etag, 0, MD5_DIGEST_LENGTH);
            return 0;
        }
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    }

    // Initialize I/O info
    http_io_init_io(priv, &io, HTTP_GET, urlbuf);
    io.block_num = block_num;

    // Allocate a buffer in case compressed and/or encrypted data is larger
    io.buf_size = compressBound(config->block_size) + EVP_MAX_IV_LENGTH;
    if ((io.dest = malloc(io.buf_size)) == NULL) {
        (*config->log)(LOG_ERR, "malloc: %s", strerror(errno));
        pthread_mutex_lock(&priv->mutex);
        priv->stats.out_of_memory_errors++;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        return ENOMEM;
    }

    // Construct URL for this block
    http_io_get_block_url(urlbuf, sizeof(urlbuf), config, block_num);

    // Add If-Match or If-None-Match header as required
    if (expect_etag != NULL && memcmp(expect_etag, zero_etag, MD5_DIGEST_LENGTH) != 0) {
        char etagbuf[MD5_DIGEST_LENGTH * 2 + 1];
        const char *header;

        if (strict)
            header = IF_MATCH_HEADER;
        else {
            header = IF_NONE_MATCH_HEADER;
            io.expect_304 = 1;
        }
        http_io_prhex(etagbuf, expect_etag, MD5_DIGEST_LENGTH);
        http_io_add_header(priv, &io, "%s: \"%s\"", header, etagbuf);
    }

    // Set Accept-Encoding header
    *accept_encoding = '\0';
    for (i = 0; i < num_comp_algs; i++) {
        const struct comp_alg *calg = &comp_algs[i];

        if (*accept_encoding != '\0')
            snvprintf(accept_encoding + strlen(accept_encoding), sizeof(accept_encoding) - strlen(accept_encoding), ", ");
        snvprintf(accept_encoding + strlen(accept_encoding), sizeof(accept_encoding) - strlen(accept_encoding), "%s", calg->name);
    }
    if (config->encryption != NULL) {
        if (*accept_encoding != '\0')
            snvprintf(accept_encoding + strlen(accept_encoding), sizeof(accept_encoding) - strlen(accept_encoding), ", ");
        snvprintf(accept_encoding + strlen(accept_encoding), sizeof(accept_encoding) - strlen(accept_encoding),
          "%s-%s", CONTENT_ENCODING_ENCRYPT, config->encryption);
    }
    if (*accept_encoding != '\0')
        http_io_add_header(priv, &io, "%s: %s", ACCEPT_ENCODING_HEADER, accept_encoding);

    // Perform operation
    r = http_io_perform_io(priv, &io, http_io_read_prepper);

    // Verify an ETag was provided by server if caller wants it
    if (r == 0 && actual_etag != NULL)
        r = http_io_verify_etag_provided(&io);

    // Determine how many bytes we read
    did_read = io.buf_size - io.bufs.rdremain;

    // Check Content-Encoding and decode if necessary
    if (*io.content_encoding == '\0' && config->default_ce != NULL)
        snvprintf(io.content_encoding, sizeof(io.content_encoding), "%s", config->default_ce);
    for ( ; r == 0 && *io.content_encoding != '\0'; *layer = '\0') {
        const struct comp_alg *calg;

        // Find next encoding layer, starting from the end and working backwards, trimming any whitespace
        if ((layer = strrchr(io.content_encoding, ',')) != NULL)
            *layer++ = '\0';
        else
            layer = io.content_encoding;
        while (isspace(*layer))
            layer++;
        while (*layer != '\0' && isspace(layer[strlen(layer) - 1]))
            layer[strlen(layer) - 1] = '\0';

        // Sanity check
        if (io.dest == NULL)
            goto bad_encoding;

        // Check for encryption (which must have been applied after compression)
        if (strncasecmp(layer, CONTENT_ENCODING_ENCRYPT "-", sizeof(CONTENT_ENCODING_ENCRYPT)) == 0) {
            const char *const block_cipher = layer + sizeof(CONTENT_ENCODING_ENCRYPT);
            u_char hmac[SHA_DIGEST_LENGTH];
            u_int decrypt_buflen;
            u_char *buf;

            // Encryption must be enabled
            if (config->encryption == NULL) {
                (*config->log)(LOG_ERR, "block %0*jx is encrypted with `%s' but `--encrypt' was not specified",
                  S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num, block_cipher);
                r = EIO;
                break;
            }

            // Verify encryption type
            if (strcasecmp(block_cipher, EVP_CIPHER_name(priv->cipher)) != 0) {
                (*config->log)(LOG_ERR, "block %0*jx was encrypted using `%s' but `%s' encryption is configured",
                  S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num, block_cipher, EVP_CIPHER_name(priv->cipher));
                r = EIO;
                break;
            }

            // Verify block's signature
            if (memcmp(io.hmac, zero_hmac, sizeof(io.hmac)) == 0) {
                (*config->log)(LOG_ERR, "block %0*jx is encrypted, but no signature was found",
                  S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
                r = EIO;
                break;
            }
            http_io_authsig(priv, block_num, io.dest, did_read, hmac);
            if (memcmp(io.hmac, hmac, sizeof(hmac)) != 0) {
                (*config->log)(LOG_ERR, "block %0*jx has an incorrect signature (did you provide the right password?)",
                  S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
                r = EIO;
                break;
            }

            // Allocate buffer for the decrypted data
            decrypt_buflen = did_read + EVP_MAX_IV_LENGTH;
            if ((buf = malloc(decrypt_buflen)) == NULL) {
                (*config->log)(LOG_ERR, "malloc: %s", strerror(errno));
                pthread_mutex_lock(&priv->mutex);
                priv->stats.out_of_memory_errors++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                r = ENOMEM;
                break;
            }

            // Decrypt the block
            did_read = http_io_crypt(priv, block_num, 0, io.dest, did_read, buf, decrypt_buflen);
            memcpy(io.dest, buf, did_read);
            free(buf);

            // Proceed
            encrypted = 1;
            continue;
        }

        // Check for compression
        if ((calg = comp_find(layer)) != NULL) {
            size_t uclen = config->block_size;

            if ((r = (*calg->dfunc)(config->log, io.dest, did_read, dest, &uclen)) != 0)  {
                if (r == ENOMEM) {
                    pthread_mutex_lock(&priv->mutex);
                    priv->stats.out_of_memory_errors++;
                    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                }
                continue;
            }

            // Update data
            did_read = uclen;
            free(io.dest);
            io.dest = NULL;         // compression should have been first, so decompression should always be last

            // Proceed
            continue;
        }

bad_encoding:
        // It was something we don't recognize
        (*config->log)(LOG_ERR, "read of block %0*jx returned unexpected encoding \"%s\"",
          S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num, layer);
        r = EIO;
        break;
    }

    // Check for required encryption
    if (r == 0 && config->encryption != NULL && !encrypted) {
        (*config->log)(LOG_ERR, "block %0*jx was supposed to be encrypted but wasn't", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
        r = EIO;
    }

    // Check for wrong length read
    if (r == 0 && did_read != config->block_size) {
        (*config->log)(LOG_ERR, "read of block %0*jx returned %lu != %lu bytes",
          S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num, (u_long)did_read, (u_long)config->block_size);
        r = EIO;
    }

    // Copy the data to the desination buffer (if we haven't already)
    if (r == 0 && io.dest != NULL)
        memcpy(dest, io.dest, config->block_size);

    // Update stats
    pthread_mutex_lock(&priv->mutex);
    switch (r) {
    case 0:
        priv->stats.normal_blocks_read++;
        break;
    case ENOENT:
        priv->stats.zero_blocks_read++;
        break;
    default:
        break;
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Check expected ETag
    if (expect_etag != NULL) {
        const int expected_not_found = memcmp(expect_etag, zero_etag, MD5_DIGEST_LENGTH) == 0;

        // Compare result with expectation
        switch (r) {
        case 0:
            if (expected_not_found)
                r = strict ? EIO : 0;
            break;
        case ENOENT:
            if (expected_not_found)
                r = strict ? 0 : EEXIST;
            break;
        default:
            break;
        }

        // Update stats
        if (!strict) {
            switch (r) {
            case 0:
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_mismatch++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                break;
            case EEXIST:
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_verified++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                break;
            default:
                break;
            }
        }
    }

    // Treat `404 Not Found' all zeros
    if (r == ENOENT) {
        memset(dest, 0, config->block_size);
        r = 0;
    }

    // Copy actual ETag
    if (actual_etag != NULL)
        memcpy(actual_etag, io.etag, MD5_DIGEST_LENGTH);

    //  Clean up
    if (io.dest != NULL)
        free(io.dest);
    curl_slist_free_all(io.headers);
    return r;
}

static int
http_io_read_prepper(struct http_io_private *const priv, CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    if (http_io_add_data_and_auth_headers(priv, io) != 0)
        return 0;
    io->bufs.rdremain = io->buf_size;
    io->bufs.rddata = io->dest;
    if (!http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEFUNCTION, http_io_curl_reader)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEDATA, io)
      || !http_io_curl_setopt_off(priv, curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)io->buf_size)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HTTPHEADER, io->headers)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HEADERFUNCTION, http_io_curl_header)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HEADERDATA, io)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_ACCEPT_ENCODING, "")
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_HTTP_CONTENT_DECODING, 0))
        return 0;
    http_io_curl_header_reset(io);
    return 1;
}

/*
 * Write block if src != NULL, otherwise delete block.
 */
static int
http_io_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *caller_etag,
  check_cancel_t *check_cancel, void *check_cancel_arg)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char urlbuf[URL_BUF_SIZE(config)];
    char hmacbuf[SHA_DIGEST_LENGTH * 2 + 1];
    u_char hmac[SHA_DIGEST_LENGTH];
    u_char md5[MD5_DIGEST_LENGTH];
    void *encoded_buf = NULL;
    struct http_io io;
    int compressed = 0;
    int encrypted = 0;
    int r;

    // Sanity check
    if (config->block_size == 0 || block_num >= config->num_blocks)
        return EINVAL;

    // Detect zero blocks (if not done already by upper layer)
    if (src != NULL && block_is_zeros(src))
        src = NULL;

    // Don't write zero blocks when bitmap indicates empty until non-zero content is written
    if (priv->non_zero != NULL) {
        pthread_mutex_lock(&priv->mutex);
        if (src == NULL) {
            if (!bitmap_test(priv->non_zero, block_num)) {
                priv->stats.empty_blocks_written++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                if (caller_etag != NULL)
                    memset(caller_etag, 0, MD5_DIGEST_LENGTH);
                return 0;
            }
        } else
            bitmap_set(priv->non_zero, block_num, 1);
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    }

    // Initialize I/O info
    http_io_init_io(priv, &io, src != NULL ? HTTP_PUT : HTTP_DELETE, urlbuf);
    io.src = src;
    io.buf_size = config->block_size;
    io.block_num = block_num;
    io.check_cancel = check_cancel;
    io.check_cancel_arg = check_cancel_arg;

    // Compress block if desired
    if (src != NULL && config->compress_alg != NULL) {
        size_t compress_len;

        // Compress data
        if ((r = (*config->compress_alg->cfunc)(config->log, io.src,
          io.buf_size, &encoded_buf, &compress_len, config->compress_level)) != 0) {
            if (r == ENOMEM) {
                pthread_mutex_lock(&priv->mutex);
                priv->stats.out_of_memory_errors++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            }
            goto fail;
        }

        // Update POST data
        io.src = encoded_buf;
        io.buf_size = compress_len;
        compressed = 1;
    }

    // Encrypt data if desired
    if (src != NULL && config->encryption != NULL) {
        void *encrypt_buf;
        u_int encrypt_len;
        u_int encrypt_buflen;

        // Allocate buffer
        encrypt_buflen = io.buf_size + EVP_MAX_IV_LENGTH;
        if ((encrypt_buf = malloc(encrypt_buflen)) == NULL) {
            (*config->log)(LOG_ERR, "malloc: %s", strerror(errno));
            pthread_mutex_lock(&priv->mutex);
            priv->stats.out_of_memory_errors++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            r = ENOMEM;
            goto fail;
        }

        // Encrypt the block
        encrypt_len = http_io_crypt(priv, block_num, 1, io.src, io.buf_size, encrypt_buf, encrypt_buflen);

        // Compute block signature
        http_io_authsig(priv, block_num, encrypt_buf, encrypt_len, hmac);
        http_io_prhex(hmacbuf, hmac, SHA_DIGEST_LENGTH);

        // Update POST data
        io.src = encrypt_buf;
        io.buf_size = encrypt_len;
        free(encoded_buf);              // OK if NULL
        encoded_buf = encrypt_buf;
        encrypted = 1;
    }

    // Set Content-Encoding HTTP header
    if (compressed || encrypted) {
        char ebuf[128];

        snvprintf(ebuf, sizeof(ebuf), "%s: ", CONTENT_ENCODING_HEADER);
        if (compressed)
            snvprintf(ebuf + strlen(ebuf), sizeof(ebuf) - strlen(ebuf), "%s", config->compress_alg->name);
        if (encrypted) {
            snvprintf(ebuf + strlen(ebuf), sizeof(ebuf) - strlen(ebuf), "%s%s-%s",
              compressed ? ", " : "", CONTENT_ENCODING_ENCRYPT, config->encryption);
        }
        http_io_add_header(priv, &io, "%s", ebuf);
    }

    // Compute MD5 checksum
    if (src != NULL)
        md5_quick(io.src, io.buf_size, md5);
    else
        memset(md5, 0, MD5_DIGEST_LENGTH);

    // Construct URL for this block
    http_io_get_block_url(urlbuf, sizeof(urlbuf), config, block_num);

    // Add PUT-only headers
    if (src != NULL) {
        char md5buf[(MD5_DIGEST_LENGTH * 4) / 3 + 4];

        // Add Content-Type header
        http_io_add_header(priv, &io, "%s: %s", CTYPE_HEADER, CONTENT_TYPE);

        // Add Content-MD5 header
        http_io_base64_encode(md5buf, sizeof(md5buf), md5, MD5_DIGEST_LENGTH);
        http_io_add_header(priv, &io, "%s: %s", MD5_HEADER, md5buf);
    }

    // Add ACL header (PUT only)
    if (src != NULL)
        http_io_add_header(priv, &io, "%s: %s", ACL_HEADER, config->accessType);

    // Add file size meta-data to zero'th block
    if (src != NULL && block_num == 0) {
        http_io_add_header(priv, &io, "%s: %u", BLOCK_SIZE_HEADER, config->block_size);
        http_io_add_header(priv, &io, "%s: %ju", FILE_SIZE_HEADER, (uintmax_t)config->block_size * (uintmax_t)config->num_blocks);
    }

    // Add signature header (if encrypting)
    if (src != NULL && config->encryption != NULL)
        http_io_add_header(priv, &io, "%s: \"%s\"", HMAC_HEADER, hmacbuf);

    // Add Server Side Encryption header(s) (if needed)
    if (config->sse != NULL && src != NULL) {
        http_io_add_header(priv, &io, "%s: %s", SSE_HEADER, config->sse);
        if (strcmp(config->sse, SSE_AWS_KMS) == 0)
            http_io_add_header(priv, &io, "%s: %s", SSE_KEY_ID_HEADER, config->sse_key_id);
    }

    // Add storage class header (if needed)
    if (config->storage_class != NULL)
        http_io_add_header(priv, &io, "%s: %s", STORAGE_CLASS_HEADER, config->storage_class);

    // Perform operation
    r = http_io_perform_io(priv, &io, http_io_write_prepper);

    // Verify ETag was provided by server if we did a PUT and caller wants it
    if (r == 0 && caller_etag != NULL && src != NULL)
        r = http_io_verify_etag_provided(&io);

    // Report ETag back to caller if requested
    if (r == 0 && caller_etag != NULL)
        memcpy(caller_etag, src != NULL ? io.etag : zero_etag, MD5_DIGEST_LENGTH);

    // Update stats
    if (r == 0) {
        pthread_mutex_lock(&priv->mutex);
        if (src == NULL)
            priv->stats.zero_blocks_written++;
        else
            priv->stats.normal_blocks_written++;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    }

fail:
    //  Clean up
    curl_slist_free_all(io.headers);
    if (encoded_buf != NULL)
        free(encoded_buf);
    return r;
}

static int
http_io_verify_etag_provided(struct http_io *const io)
{
    if (memcmp(io->etag, zero_etag, MD5_DIGEST_LENGTH) == 0) {
        (*io->config->log)(LOG_ERR, "%s %s: missing %s header in response", io->method, io->url, ETAG_HEADER);
        return EIO;
    }
    return 0;
}

static int
http_io_write_prepper(struct http_io_private *const priv, CURL *curl, struct http_io *io)
{
    memset(&io->bufs, 0, sizeof(io->bufs));
    if (http_io_add_data_and_auth_headers(priv, io) != 0)
        return 0;
    if (io->src != NULL) {
        io->bufs.wrremain = io->buf_size;
        io->bufs.wrdata = io->src;
    }
    if (!http_io_curl_setopt_ptr(priv, curl, CURLOPT_READFUNCTION, http_io_curl_writer)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_READDATA, io)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEFUNCTION, http_io_curl_reader)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_WRITEDATA, io)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HEADERFUNCTION, http_io_curl_header)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HEADERDATA, io)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_CUSTOMREQUEST, io->method)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_HTTPHEADER, io->headers))
        return 0;
    if (io->src != NULL) {
        if (!http_io_curl_setopt_long(priv, curl, CURLOPT_UPLOAD, 1)
          || !http_io_curl_setopt_off(priv, curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)io->buf_size))
            return 0;
    }
    http_io_curl_header_reset(io);
    return 1;
}

static int
http_io_bulk_zero(struct s3backer_store *const s3b, const s3b_block_t *block_nums, u_int num_blocks)
{
    struct http_io_private *const priv = s3b->data;
    struct http_io_conf *const config = priv->config;
    char urlbuf[URL_BUF_SIZE(config)];
    size_t max_object_name;
    struct http_io io;
    char *buf = NULL;
    int r;

    // Set URL
    snvprintf(urlbuf, sizeof(urlbuf), "%s?delete", config->vhostURL);

    // Initialize XML query
    if ((r = http_io_xml_io_init(priv, &io, HTTP_POST, urlbuf)) != 0)
        return r;

    // Calculate the maximum length of one object name
    max_object_name = strlen(config->prefix) + S3B_BLOCK_NUM_DIGITS
      + strlen(BLOCK_HASH_PREFIX_SEPARATOR) + S3B_BLOCK_NUM_DIGITS + 1;           // [PREFIX]([HASH][SEPARATOR])?[BLOCK]

    // Delete blocks in chunks of DELETE_BLOCKS_CHUNK
    while (num_blocks > 0) {
        const int count = num_blocks <= DELETE_BLOCKS_CHUNK ? num_blocks : DELETE_BLOCKS_CHUNK;
        size_t max_object_elem;
        size_t max_payload;
        size_t payload_len;
        char *new_buf;
        u_int i;

        // Calculate upper bound on size of query payload
        max_object_elem = strlen(DELETE_ELEM_OBJECT) + 2                                            // <Object>
          + strlen(DELETE_ELEM_KEY) + 2 + max_object_name + strlen(DELETE_ELEM_KEY) + 3             //   <Key>xxx</Key>
          + strlen(DELETE_ELEM_OBJECT) + 3;                                                         // </Object>
        max_payload = strlen(DELETE_ELEM_DELETE) + 2                                                // <Delete>
          + (count * max_object_elem)                                                               //   <Object>...
          + strlen(DELETE_ELEM_DELETE) + 3                                                          // </Delete>
          + 2;                                                                                      // nul byte, plus one extra

        // Allocate buffer
        if ((new_buf = realloc(buf, max_payload)) == NULL) {
            r = errno;
            (*config->log)(LOG_ERR, "realloc: %s", strerror(r));
            break;
        }
        buf = new_buf;

        // Build XML payload
        payload_len = snvprintf(buf, max_payload, "<%s>", DELETE_ELEM_DELETE);
        for (i = 0; i < count; i++) {
            const s3b_block_t block_num = block_nums[i];
            char block_hash_buf[S3B_BLOCK_NUM_DIGITS + strlen(BLOCK_HASH_PREFIX_SEPARATOR) + 1];
            char object_buf[max_object_name + 1];

            http_io_format_block_hash(config->blockHashPrefix, block_hash_buf, sizeof(block_hash_buf), block_num);
            snvprintf(object_buf, sizeof(object_buf), "%s%s%0*jx",
              config->prefix, block_hash_buf, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
            payload_len += snvprintf(buf + payload_len, max_payload - payload_len, "<%s><%s>%s</%s></%s>",
              DELETE_ELEM_OBJECT, DELETE_ELEM_KEY, object_buf, DELETE_ELEM_KEY, DELETE_ELEM_OBJECT);
        }
        payload_len += snvprintf(buf + payload_len, max_payload - payload_len, "</%s>", DELETE_ELEM_DELETE);

        // Initialize buffer
        io.src = buf;
        io.buf_size = payload_len;

        // Perform operation
        if ((r = http_io_xml_io_exec(priv, &io, http_io_bulk_delete_elem_end)) != 0)
            break;

        // Advance
        block_nums += count;
        num_blocks -= count;
    }

    // Done
    http_io_xml_io_destroy(priv, &io);
    free(buf);
    return r;
}

static void
http_io_bulk_delete_elem_end(void *arg, const XML_Char *name)
{
    struct http_io *const io = (struct http_io *)arg;
    struct http_io_conf *const config = io->config;
    char **copy_ptr = NULL;

    // If we are in an error state, just bail
    if (io->xml_error != XML_ERROR_NONE || io->handler_error != 0)
        return;

    // Handle <DeleteResult>/<Error>/<Foo> tags
    if (strcmp(io->xml_path, "/" DELETE_ELEM_DELETE_RESULT "/" DELETE_ELEM_ERROR "/" DELETE_ELEM_KEY) == 0)
        copy_ptr = &io->bulk_delete_err_key;
    else if (strcmp(io->xml_path, "/" DELETE_ELEM_DELETE_RESULT "/" DELETE_ELEM_ERROR "/" DELETE_ELEM_CODE) == 0)
        copy_ptr = &io->bulk_delete_err_code;
    else if (strcmp(io->xml_path, "/" DELETE_ELEM_DELETE_RESULT "/" DELETE_ELEM_ERROR "/" DELETE_ELEM_MESSAGE) == 0)
        copy_ptr = &io->bulk_delete_err_msg;
    if (copy_ptr != NULL) {
        free(*copy_ptr);
        if ((*copy_ptr = strdup(io->xml_text)) == NULL) {
            io->handler_error = errno;
            (*config->log)(LOG_ERR, "strdup: %s", strerror(errno));
        }
        goto done;
    }

    // Handle end of <DeleteResult>/<Error>
    if (strcmp(io->xml_path, "/" DELETE_ELEM_DELETE_RESULT "/" DELETE_ELEM_ERROR) == 0) {
        (*config->log)(LOG_ERR, "bulk delete error: key=\"%s\" code=\"%s\" message=\"%s\"",
          io->bulk_delete_err_key != NULL ? io->bulk_delete_err_key : "(none)",
          io->bulk_delete_err_code != NULL ? io->bulk_delete_err_code : "(none)",
          io->bulk_delete_err_msg != NULL ? io->bulk_delete_err_msg : "(none)");
        io->handler_error = EIO;
        goto done;
    }

done:
    // Update current XML path
    assert(strrchr(io->xml_path, '/') != NULL);
    *strrchr(io->xml_path, '/') = '\0';

    // Reset text buffer
    io->xml_text[0] = '\0';
}

static int
http_io_xml_io_init(struct http_io_private *const priv, struct http_io *io, const char *method, char *url)
{
    struct http_io_conf *const config = priv->config;
    int r;

    // Initialize I/O info
    http_io_init_io(priv, io, method, url);
    io->xml_error = XML_ERROR_NONE;

    // Create XML parser
    if ((io->xml = XML_ParserCreate(NULL)) == NULL) {
        (*config->log)(LOG_ERR, "failed to create XML parser");
        r = ENOMEM;
        goto fail;
    }

    // Allocate buffers for XML path and tag text content
    if ((io->xml_text = calloc(1, 1)) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc: %s", strerror(r));
        goto fail;
    }
    if ((io->xml_path = calloc(1, 1)) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc: %s", strerror(r));
        goto fail;
    }

    // OK
    return 0;

fail:
    // Update stats
    if (r == ENOMEM) {
        pthread_mutex_lock(&priv->mutex);
        priv->stats.out_of_memory_errors++;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    }

    // Cleanup
    http_io_xml_io_destroy(priv, io);
    return r;
}

static int
http_io_xml_io_exec(struct http_io_private *const priv, struct http_io *io, void (*end_handler)(void *arg, const XML_Char *name))
{
    struct http_io_conf *const config = priv->config;
    int r;

    // Reset XML parser state
    XML_ParserReset(io->xml, NULL);
    XML_SetUserData(io->xml, io);
    XML_SetElementHandler(io->xml, http_io_xml_elem_start, end_handler);
    XML_SetCharacterDataHandler(io->xml, http_io_xml_text);

    // Add headers for payload (if needed)
    if (io->src != NULL) {
        u_char md5[MD5_DIGEST_LENGTH];
        char md5buf[MD5_DIGEST_LENGTH * 2 + 1];

        // Add Content-MD5 header
        md5_quick(io->src, io->buf_size, md5);
        http_io_base64_encode(md5buf, sizeof(md5buf), md5, MD5_DIGEST_LENGTH);
        http_io_add_header(priv, io, "%s: %s", MD5_HEADER, md5buf);

        // Add Content-Type header
        http_io_add_header(priv, io, "%s: %s", CTYPE_HEADER, "application/xml");
    }

    // Perform operation
    r = http_io_perform_io(priv, io, http_io_xml_prepper);

    // Clean up headers
    curl_slist_free_all(io->headers);
    io->headers = NULL;

    // Check for error from curl
    if (r != 0)
        return r;

    // Check for error from handlers
    if ((r = io->handler_error) != 0)
        return r;

    // Finalize parse (if not already busted)
    if (io->xml_error == XML_ERROR_NONE && XML_Parse(io->xml, NULL, 0, 1) != XML_STATUS_OK) {
        io->xml_error = XML_GetErrorCode(io->xml);
        io->xml_error_line = XML_GetCurrentLineNumber(io->xml);
        io->xml_error_column = XML_GetCurrentColumnNumber(io->xml);
    }

    // Check for XML error
    if (io->xml_error != XML_ERROR_NONE) {
        (*config->log)(LOG_ERR, "XML parse error: line %d col %d: %s",
          io->xml_error_line, io->xml_error_column, XML_ErrorString(io->xml_error));
        return EIO;
    }

    // Done
    return 0;
}

static void
http_io_xml_io_destroy(struct http_io_private *const priv, struct http_io *io)
{
    if (io->xml != NULL) {
        XML_ParserFree(io->xml);
        io->xml = NULL;
    }
    free(io->xml_path);
    io->xml_path = NULL;
    free(io->xml_text);
    io->xml_text = NULL;
}

/*
 * Perform HTTP operation.
 */
static int
http_io_perform_io(struct http_io_private *priv, struct http_io *io, http_io_curl_prepper_t *prepper)
{
    struct http_io_conf *const config = priv->config;
    struct http_io_bufs obufs;
    struct timespec delay;
    CURLcode curl_code;
    int last_error = EIO;
    u_int retry_pause = 0;
    u_int total_pause;
    long http_code;
    int may_cache;
    double clen;
    int attempt;
    CURL *curl;

    // Snapshot payload buffers now so we can reset on retry
    memcpy(&obufs, &io->bufs, sizeof(io->bufs));

    // Make attempts
    for (attempt = 0, total_pause = 0; 1; attempt++, total_pause += retry_pause) {

        // Reset upload and download payloads on retry
        if (attempt > 0)
            memcpy(&io->bufs, &obufs, sizeof(io->bufs));

        // Debug
        if (config->debug) {
            (*config->log)(LOG_DEBUG, "%s %s", io->method, io->url);
            if (config->debug_http && io->src != NULL) {
                size_t chars_to_print = io->buf_size;

                if (chars_to_print > MAX_DEBUG_PAYLOAD_SIZE)
                    chars_to_print = MAX_DEBUG_PAYLOAD_SIZE;
                (*config->log)(LOG_DEBUG, "HTTP %s request payload:\n%.*s",
                  io->method, (int)chars_to_print, (const char *)io->src);
            }
        }

        // Acquire and initialize CURL instance
        if ((curl = http_io_acquire_curl(priv, io)) == NULL)
            return EIO;
        if (!(*prepper)(priv, curl, io))
            return EIO;

        // Reset error payload capture
        io->http_status = 0;
        assert(io->error_payload == NULL);
        assert(io->error_payload_len == 0);

        // Perform HTTP operation and check result
        if (attempt > 0)
            (*config->log)(LOG_INFO, "retrying query (attempt #%d): %s %s", attempt + 1, io->method, io->url);
        io->curl = curl;
        curl_code = curl_easy_perform(curl);
        io->curl = NULL;

        // Find out what the HTTP result code was (if any)
        switch (curl_code) {
        case CURLE_HTTP_RETURNED_ERROR:                         // should never happen (we no longer use CURLOPT_FAILONERROR)
        case 0:
            if (curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code) != 0)
                http_code = 999;                                // this should never happen
            break;
        default:
            http_code = -1;
            break;
        }

        // Pretend like the CURLOPT_FAILONERROR option was used
        if (curl_code == 0 && http_code >= HTTP_STATUS_ERROR_MINIMUM)
            curl_code = CURLE_HTTP_RETURNED_ERROR;

        // In the case of a DELETE, treat an HTTP_NOT_FOUND error as successful
        if (curl_code == CURLE_HTTP_RETURNED_ERROR
          && http_code == HTTP_NOT_FOUND
          && strcmp(io->method, HTTP_DELETE) == 0)
            curl_code = 0;

        // Handle success
        if (curl_code == 0) {
            double curl_time;
            int r = 0;

            // Discard any error payload (e.g., 404 Not Found from DELETE)
            http_io_free_error_payload(io);

            // Extra debug logging
            if (config->debug)
                (*config->log)(LOG_DEBUG, "success: %s %s", io->method, io->url);

            // Extract timing info
            if ((curl_code = curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &curl_time)) != CURLE_OK) {
                (*config->log)(LOG_ERR, "can't get cURL timing: %s", curl_easy_strerror(curl_code));
                curl_time = 0.0;
            }

            // Extract content-length (if required)
            if (io->content_lengthp != NULL) {
                if ((curl_code = curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &clen)) == CURLE_OK)
                    *io->content_lengthp = (u_int)clen;
                else {
                    (*config->log)(LOG_ERR, "can't get content-length: %s", curl_easy_strerror(curl_code));
                    r = ENXIO;
                }
            }

            // Update stats
            pthread_mutex_lock(&priv->mutex);
            if (strcmp(io->method, HTTP_GET) == 0) {
                priv->stats.http_gets.count++;
                priv->stats.http_gets.time += curl_time;
            } else if (strcmp(io->method, HTTP_PUT) == 0) {
                priv->stats.http_puts.count++;
                priv->stats.http_puts.time += curl_time;
            } else if (strcmp(io->method, HTTP_DELETE) == 0) {
                priv->stats.http_deletes.count++;
                priv->stats.http_deletes.time += curl_time;
            } else if (strcmp(io->method, HTTP_HEAD) == 0) {
                priv->stats.http_heads.count++;
                priv->stats.http_heads.time += curl_time;
            }
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

            // Done
            http_io_release_curl(priv, &curl, r == 0);
            return r;
        }

        // Determine whether we think it's safe to re-use the curl handle after an error
        may_cache = http_io_safe_to_cache_curl_handle(curl_code, http_code);

        // Free the curl handle (and don't cache it if connection might be broken)
        http_io_release_curl(priv, &curl, may_cache);

        // Handle errors
        switch (curl_code) {
        case CURLE_ABORTED_BY_CALLBACK:
            if (config->debug)
                (*config->log)(LOG_DEBUG, "write aborted: %s %s", io->method, io->url);
            pthread_mutex_lock(&priv->mutex);
            priv->stats.http_canceled_writes++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            http_io_free_error_payload(io);
            return ECONNABORTED;
        case CURLE_OPERATION_TIMEDOUT:
            (*config->log)(LOG_NOTICE, "operation timeout: %s %s", io->method, io->url);
            pthread_mutex_lock(&priv->mutex);
            priv->stats.curl_timeouts++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            last_error = ETIMEDOUT;
            break;
        case CURLE_HTTP_RETURNED_ERROR:                 // special handling for some specific HTTP codes
            switch (http_code) {
            case HTTP_NOT_FOUND:
                if (config->debug)
                    (*config->log)(LOG_DEBUG, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                http_io_free_error_payload(io);
                return ENOENT;
            case HTTP_UNAUTHORIZED:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_unauthorized++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                http_io_log_error_payload(io);
                last_error = EACCES;
                break;
            case HTTP_FORBIDDEN:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_forbidden++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                http_io_log_error_payload(io);
                last_error = EPERM;
                break;
            case HTTP_PRECONDITION_FAILED:
                (*config->log)(LOG_INFO, "rec'd stale content: %s %s", io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_stale++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                last_error = ESTALE;
                break;
            case HTTP_MOVED_PERMANENTLY:
            case HTTP_FOUND:
            case HTTP_TEMPORARY_REDIRECT:
            case HTTP_PERMANENT_REDIRECT:
                (*config->log)(LOG_ERR, "rec'd %ld redirect: %s %s", http_code, io->method, io->url);
                (*config->log)(LOG_ERR, "hint: you may need the \"--vhost\" and/or \"--region\" flags");
                pthread_mutex_lock(&priv->mutex);
                priv->stats.http_redirect++;
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                last_error = ELOOP;
                break;
            case HTTP_NOT_MODIFIED:
                if (io->expect_304) {
                    if (config->debug)
                        (*config->log)(LOG_DEBUG, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                    return EEXIST;
                }
                // FALLTHROUGH
            default:
                (*config->log)(LOG_ERR, "rec'd %ld response: %s %s", http_code, io->method, io->url);
                pthread_mutex_lock(&priv->mutex);
                switch (http_code / 100) {
                case 3:
                    priv->stats.http_3xx_error++;
                    break;
                case 4:
                    priv->stats.http_4xx_error++;
                    break;
                case 5:
                    priv->stats.http_5xx_error++;
                    break;
                default:
                    priv->stats.http_other_error++;
                    break;
                }
                CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
                http_io_log_error_payload(io);
                last_error = EIO;
                break;
            }
            break;
        default:
            (*config->log)(LOG_ERR, "operation failed: %s (%s)", curl_easy_strerror(curl_code),
              total_pause >= config->max_retry_pause ? "final attempt" : "will retry");
            pthread_mutex_lock(&priv->mutex);
            switch (curl_code) {
            case CURLE_OUT_OF_MEMORY:
                priv->stats.curl_out_of_memory++;
                last_error = ENOMEM;
                break;
            case CURLE_COULDNT_CONNECT:
                priv->stats.curl_connect_failed++;
                last_error = ENXIO;
                break;
            case CURLE_COULDNT_RESOLVE_HOST:
                priv->stats.curl_host_unknown++;
                last_error = ENXIO;
                break;
            default:
                priv->stats.curl_other_error++;
                last_error = EIO;
                break;
            }
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            break;
        }

        // Free any error payload
        http_io_free_error_payload(io);

        // Retry with exponential backoff up to max total pause limit
        if (total_pause >= config->max_retry_pause)
            break;
        retry_pause = retry_pause > 0 ? retry_pause * 2 : config->initial_retry_pause;
        if (total_pause + retry_pause > config->max_retry_pause)
            retry_pause = config->max_retry_pause - total_pause;
        delay.tv_sec = retry_pause / 1000;
        delay.tv_nsec = (retry_pause % 1000) * 1000000;
        nanosleep(&delay, NULL);            // TODO: check for EINTR

        // Update retry stats
        pthread_mutex_lock(&priv->mutex);
        priv->stats.num_retries++;
        priv->stats.retry_delay += retry_pause;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
    }

    // Give up
    (*config->log)(LOG_ERR, "giving up on: %s %s", io->method, io->url);
    return last_error;
}

/*
 * Add Date and Authorization headers, replacing any previous.
 *
 * This should be invoked for every HTTP attempt in order to keep the request timestamp current.
 */
static int
http_io_add_data_and_auth_headers(struct http_io_private *priv, struct http_io *io)
{
    static const char *const remove_headers[] = {
        AUTH_HEADER,
        AWS_DATE_HEADER,
        CONTENT_SHA256_HEADER,
        HTTP_DATE_HEADER,
        SECURITY_TOKEN_HEADER,
        NULL
    };
    struct http_io_conf *const config = priv->config;
    struct curl_slist **current_headerp;
    const time_t now = time(NULL);
    char datebuf[DATE_BUF_SIZE];
    struct tm tm;
    int r;

    // Remove the previous date and auth headers (only happens on retries)
    current_headerp = &io->headers;
    while (*current_headerp != NULL) {
        struct curl_slist *const current_header = *current_headerp;
        const char *const header = current_header->data;
        int i;

        // Does this header need to be removed?
        for (i = 0; remove_headers[i] != NULL; i++) {
            const char *const remove_header = remove_headers[i];
            const size_t remove_header_len = strlen(remove_header);

            if (strncasecmp(header, remove_header, remove_header_len) == 0
              && header[remove_header_len] == ':')
                break;
        }

        // If so, excise header from the list and free it, otherwise proceed to the next header
        if (remove_headers[i] != NULL) {
            *current_headerp = current_header->next;
            current_header->next = NULL;
            curl_slist_free_all(current_header);
        } else
            current_headerp = &current_header->next;
    }

    // Add Date header
    if (strcmp(config->authVersion, AUTH_VERSION_AWS2) == 0) {
        strftime(datebuf, sizeof(datebuf), HTTP_DATE_BUF_FMT, gmtime_r(&now, &tm));
        if ((r = http_io_add_header(priv, io, "%s: %s", HTTP_DATE_HEADER, datebuf)) != 0)
            return r;
    } else {
        strftime(datebuf, sizeof(datebuf), AWS_DATE_BUF_FMT, gmtime_r(&now, &tm));
        if ((r = http_io_add_header(priv, io, "%s: %s", AWS_DATE_HEADER, datebuf)) != 0)
            return r;
    }

    // Add Authorization header
    if ((r = http_io_add_auth(priv, io, now, io->src, io->src != NULL ? io->buf_size : 0)) != 0)
        return r;

    // Done
    return 0;
}

/*
 * Compute S3 authorization hash using secret access key and add Authorization and SHA256 hash headers.
 *
 * Note: headers must be unique and not wrapped.
 */
static int
http_io_add_auth(struct http_io_private *priv, struct http_io *const io, time_t now, const void *payload, size_t plen)
{
    const struct http_io_conf *const config = priv->config;

    // Anything to do?
    if (config->accessId == NULL)
        return 0;

    // Which auth version?
    if (strcmp(config->authVersion, AUTH_VERSION_AWS2) == 0)
        return http_io_add_auth2(priv, io, now, payload, plen);
    if (strcmp(config->authVersion, AUTH_VERSION_AWS4) == 0)
        return http_io_add_auth4(priv, io, now, payload, plen);

    // Oops
    return EINVAL;
}

/**
 * AWS verison 2 authentication
 */
static int
http_io_add_auth2(struct http_io_private *priv, struct http_io *const io, time_t now, const void *payload, size_t plen)
{
    const struct http_io_conf *const config = priv->config;
    const struct curl_slist *header;
    u_char hmac_result[SHA_DIGEST_LENGTH];
    const char *resource;
    char **amz_hdrs = NULL;
    char access_id[128];
    char access_key[128];
    char authbuf[200];
#if DEBUG_AUTHENTICATION
    char sigbuf[1024];
    char hmac_buf[SHA_DIGEST_LENGTH * 2 + 1];
#else
    char sigbuf[1];
#endif
    int num_amz_hdrs;
    const char *qmark;
    size_t resource_len;
    struct hmac_ctx* hmac_ctx = NULL;
    int i;
    int r;

    // Snapshot current credentials
    pthread_mutex_lock(&priv->mutex);
    snvprintf(access_id, sizeof(access_id), "%s", config->accessId);
    snvprintf(access_key, sizeof(access_key), "%s", config->accessKey);
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Initialize HMAC
    if ((hmac_ctx = hmac_new_sha1(priv->hmac, (const u_char *)access_key, strlen(access_key))) == NULL) {
        r = errno;
        goto fail;
    }

#if DEBUG_AUTHENTICATION
    *sigbuf = '\0';
#endif

    // Sign initial stuff
    hmac_update(hmac_ctx, (const u_char *)io->method, strlen(io->method));
    hmac_update(hmac_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s\n", io->method);
#endif
    update_hmac_from_header(hmac_ctx, io, MD5_HEADER, 1, sigbuf, sizeof(sigbuf));
    update_hmac_from_header(hmac_ctx, io, CTYPE_HEADER, 1, sigbuf, sizeof(sigbuf));
    update_hmac_from_header(hmac_ctx, io, HTTP_DATE_HEADER, 1, sigbuf, sizeof(sigbuf));

    // Get x-amz headers sorted by name
    for (header = io->headers, num_amz_hdrs = 0; header != NULL; header = header->next) {
        if (strncmp(header->data, "x-amz", 5) == 0)
            num_amz_hdrs++;
    }
    if ((amz_hdrs = malloc(num_amz_hdrs * sizeof(*amz_hdrs))) == NULL) {
        r = errno;
        goto fail;
    }
    for (header = io->headers, i = 0; header != NULL; header = header->next) {
        if (strncmp(header->data, "x-amz", 5) == 0)
            amz_hdrs[i++] = header->data;
    }
    assert(i == num_amz_hdrs);
    qsort(amz_hdrs, num_amz_hdrs, sizeof(*amz_hdrs), http_io_header_name_sort);

    // Sign x-amz headers (in sorted order)
    for (i = 0; i < num_amz_hdrs; i++)
        update_hmac_from_header(hmac_ctx, io, amz_hdrs[i], 0, sigbuf, sizeof(sigbuf));

    // Get resource
    resource = config->vhost ? io->url + strlen(config->baseURL) - 1 : io->url + strlen(config->baseURL) + strlen(config->bucket);
    resource_len = (qmark = strchr(resource, '?')) != NULL ? qmark - resource : strlen(resource);

    // Sign final stuff
    hmac_update(hmac_ctx, (const u_char *)"/", 1);
    hmac_update(hmac_ctx, (const u_char *)config->bucket, strlen(config->bucket));
    hmac_update(hmac_ctx, (const u_char *)resource, resource_len);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "/%s%.*s", config->bucket, (int)resource_len, resource);
#endif

    // Finish up
    hmac_final(hmac_ctx, hmac_result);

    // Base64-encode result
    http_io_base64_encode(authbuf, sizeof(authbuf), hmac_result, sizeof(hmac_result));

#if DEBUG_AUTHENTICATION
    (*config->log)(LOG_DEBUG, "auth: string to sign:\n%s", sigbuf);
    http_io_prhex(hmac_buf, hmac_result, sizeof(hmac_result));
    (*config->log)(LOG_DEBUG, "auth: signature hmac = %s", hmac_buf);
    (*config->log)(LOG_DEBUG, "auth: signature hmac base64 = %s", authbuf);
#endif

    // Add auth header
    http_io_add_header(priv, io, "%s: AWS %s:%s", AUTH_HEADER, access_id, authbuf);

    // Done
    r = 0;

fail:
    // Clean up
    if (amz_hdrs != NULL)
        free(amz_hdrs);
    hmac_free(hmac_ctx);
    return r;
}

/**
 * AWS verison 4 authentication
 */
static int
http_io_add_auth4(struct http_io_private *priv, struct http_io *const io, time_t now, const void *payload, size_t plen)
{
    const struct http_io_conf *const config = priv->config;
    u_char payload_hash[SHA256_DIGEST_LENGTH];
    u_char creq_hash[SHA256_DIGEST_LENGTH];
    u_char hmac_result[SHA256_DIGEST_LENGTH];
    u_int payload_hash_len;
    u_int creq_hash_len;
    char payload_hash_buf[SHA256_DIGEST_LENGTH * 2 + 1];
    char creq_hash_buf[SHA256_DIGEST_LENGTH * 2 + 1];
    char hmac_buf[SHA256_DIGEST_LENGTH * 2 + 1];
    const struct curl_slist *hdr;
    char **sorted_hdrs = NULL;
    char *header_names = NULL;
    const char *host;
    size_t host_len;
    const char *uripath;
    size_t uripath_len;
    const char *query_params;
    size_t query_params_len;
    u_int header_names_length;
    u_int num_sorted_hdrs;
    EVP_MD_CTX* hash_ctx;
    struct hmac_ctx* hmac_ctx = NULL;
#if DEBUG_AUTHENTICATION
    char sigbuf[1024];
#endif
    char hosthdr[128];
    char datebuf[DATE_BUF_SIZE];
    char access_id[128];
    char access_key[128];
    char *iam_token = NULL;
    char *cquery;
    struct tm tm;
    char *p;
    int r;
    int i;

    // Initialize
    hash_ctx = EVP_MD_CTX_new();
    assert(hash_ctx != NULL);

    // Snapshot current credentials
    pthread_mutex_lock(&priv->mutex);
    snvprintf(access_id, sizeof(access_id), "%s", config->accessId);
    snvprintf(access_key, sizeof(access_key), "%s%s", ACCESS_KEY_PREFIX, config->accessKey);
    if (config->iam_token != NULL && (iam_token = strdup(config->iam_token)) == NULL) {
        r = errno;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        (*config->log)(LOG_ERR, "%s: strdup: %s", "http_io_add_auth4", strerror(r));
        goto fail;
    }
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));

    // Extract host, URI path, and query parameters from URL
    if ((p = strchr(io->url, ':')) == NULL || *++p != '/' || *++p != '/'
      || (host = p + 1) == NULL || (uripath = strchr(host, '/')) == NULL) {
        r = EINVAL;
        free(iam_token);
        goto fail;
    }
    host_len = uripath - host;
    if ((p = strchr(uripath, '?')) != NULL) {
        uripath_len = p - uripath;
        query_params = p + 1;
        query_params_len = strlen(query_params);
    } else {
        uripath_len = strlen(uripath);
        query_params = NULL;
        query_params_len = 0;
    }

    // Format date
    strftime(datebuf, sizeof(datebuf), AWS_DATE_BUF_FMT, gmtime_r(&now, &tm));

/****** Hash Payload and Add Header ******/

    EVP_DigestInit_ex(hash_ctx, EVP_sha256(), NULL);
    if (payload != NULL)
        EVP_DigestUpdate(hash_ctx, payload, plen);
    EVP_DigestFinal_ex(hash_ctx, payload_hash, &payload_hash_len);
    http_io_prhex(payload_hash_buf, payload_hash, payload_hash_len);

    http_io_add_header(priv, io, "%s: %s", CONTENT_SHA256_HEADER, payload_hash_buf);

/****** Add IAM security token header (if any) ******/

    if (iam_token != NULL && *iam_token != '\0') {
        http_io_add_header(priv, io, "%s: %s", SECURITY_TOKEN_HEADER, iam_token);
        free(iam_token);
    }

/****** Create Hashed Canonical Request ******/

#if DEBUG_AUTHENTICATION
    *sigbuf = '\0';
#endif

    // Reset hash
    EVP_DigestInit_ex(hash_ctx, EVP_sha256(), NULL);

    // Sort headers by (lowercase) name; add "Host" header manually - special case because cURL adds it, not us
    snvprintf(hosthdr, sizeof(hosthdr), "host:%.*s", (int)host_len, host);
    for (num_sorted_hdrs = 1, hdr = io->headers; hdr != NULL; hdr = hdr->next)
        num_sorted_hdrs++;
    if ((sorted_hdrs = malloc(num_sorted_hdrs * sizeof(*sorted_hdrs))) == NULL) {
        r = errno;
        goto fail;
    }
    sorted_hdrs[0] = hosthdr;
    for (i = 1, hdr = io->headers; hdr != NULL; hdr = hdr->next)
        sorted_hdrs[i++] = hdr->data;
    assert(i == num_sorted_hdrs);
    qsort(sorted_hdrs, num_sorted_hdrs, sizeof(*sorted_hdrs), http_io_header_name_sort);

    // Request method
    EVP_DigestUpdate(hash_ctx, (const u_char *)io->method, strlen(io->method));
    EVP_DigestUpdate(hash_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s\n", io->method);
#endif

    // Canonical URI
    digest_url_encoded(hash_ctx, uripath, uripath_len, 0);
    EVP_DigestUpdate(hash_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%.*s\n", (int)uripath_len, uripath);
#endif

    // Canonical query string
    if ((cquery = canonicalize_query_string(query_params, query_params_len)) == NULL) {
        r = errno;
        goto fail;
    }
    EVP_DigestUpdate(hash_ctx, (const u_char *)cquery, strlen(cquery));
    EVP_DigestUpdate(hash_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s\n", cquery);
#endif
    free(cquery);

    // Canonical headers
    header_names_length = 0;
    for (i = 0; i < num_sorted_hdrs; i++) {
        const char *value = sorted_hdrs[i];
        const char *s;
        char lcase;

        s = value;
        do {
            if (*s == '\0') {
                r = EINVAL;
                goto fail;
            }
            lcase = tolower(*s);
            EVP_DigestUpdate(hash_ctx, (const u_char *)&lcase, 1);
#if DEBUG_AUTHENTICATION
            snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%c", lcase);
#endif
            header_names_length++;
        } while (*s++ != ':');
        while (isspace(*s))
            s++;
        EVP_DigestUpdate(hash_ctx, (const u_char *)s, strlen(s));
        EVP_DigestUpdate(hash_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
        snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s\n", s);
#endif
    }
    EVP_DigestUpdate(hash_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "\n");
#endif

    // Signed headers
    if ((header_names = malloc(header_names_length)) == NULL) {
        r = errno;
        goto fail;
    }
    p = header_names;
    for (i = 0; i < num_sorted_hdrs; i++) {
        const char *value = sorted_hdrs[i];
        const char *s;

        if (p > header_names)
            *p++ = ';';
        for (s = value; *s != '\0' && *s != ':'; s++)
            *p++ = tolower(*s);
    }
    *p++ = '\0';
    assert(p <= header_names + header_names_length);
    EVP_DigestUpdate(hash_ctx, (const u_char *)header_names, strlen(header_names));
    EVP_DigestUpdate(hash_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s\n", header_names);
#endif

    // Hashed payload
    EVP_DigestUpdate(hash_ctx, (const u_char *)payload_hash_buf, strlen(payload_hash_buf));
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s", payload_hash_buf);
#endif

    // Get canonical request hash as a string
    EVP_DigestFinal_ex(hash_ctx, creq_hash, &creq_hash_len);
    http_io_prhex(creq_hash_buf, creq_hash, creq_hash_len);

#if DEBUG_AUTHENTICATION
    (*config->log)(LOG_DEBUG, "auth: canonical request:\n%s", sigbuf);
    (*config->log)(LOG_DEBUG, "auth: canonical request hash = %s", creq_hash_buf);
#endif

/****** Derive Signing Key ******/

    // Do nested HMAC's
    if ((hmac_ctx = hmac_new_sha256(priv->hmac, access_key, strlen(access_key))) == NULL) {
        r = errno;
        goto fail;
    }
#if DEBUG_AUTHENTICATION
    (*config->log)(LOG_DEBUG, "auth: access_key = \"%s\"", access_key);
#endif
    hmac_update(hmac_ctx, (const u_char *)datebuf, 8);
    hmac_final(hmac_ctx, hmac_result);
#if DEBUG_AUTHENTICATION
    http_io_prhex(hmac_buf, hmac_result, sizeof(hmac_result));
    (*config->log)(LOG_DEBUG, "auth: HMAC[%.8s] = %s", datebuf, hmac_buf);
#endif
    hmac_reset(hmac_ctx, hmac_result, sizeof(hmac_result));
    hmac_update(hmac_ctx, (const u_char *)config->region, strlen(config->region));
    hmac_final(hmac_ctx, hmac_result);
#if DEBUG_AUTHENTICATION
    http_io_prhex(hmac_buf, hmac_result, sizeof(hmac_result));
    (*config->log)(LOG_DEBUG, "auth: HMAC[%s] = %s", config->region, hmac_buf);
#endif
    hmac_reset(hmac_ctx, hmac_result, sizeof(hmac_result));
    hmac_update(hmac_ctx, (const u_char *)S3_SERVICE_NAME, strlen(S3_SERVICE_NAME));
    hmac_final(hmac_ctx, hmac_result);
#if DEBUG_AUTHENTICATION
    http_io_prhex(hmac_buf, hmac_result, sizeof(hmac_result));
    (*config->log)(LOG_DEBUG, "auth: HMAC[%s] = %sn", S3_SERVICE_NAME, hmac_buf);
#endif
    hmac_reset(hmac_ctx, hmac_result, sizeof(hmac_result));
    hmac_update(hmac_ctx, (const u_char *)SIGNATURE_TERMINATOR, strlen(SIGNATURE_TERMINATOR));
    hmac_final(hmac_ctx, hmac_result);
#if DEBUG_AUTHENTICATION
    http_io_prhex(hmac_buf, hmac_result, sizeof(hmac_result));
    (*config->log)(LOG_DEBUG, "auth: HMAC[%s] = %s", SIGNATURE_TERMINATOR, hmac_buf);
#endif

/****** Sign the String To Sign ******/

#if DEBUG_AUTHENTICATION
    *sigbuf = '\0';
#endif
    hmac_reset(hmac_ctx, hmac_result, sizeof(hmac_result));
    hmac_update(hmac_ctx, (const u_char *)SIGNATURE_ALGORITHM, strlen(SIGNATURE_ALGORITHM));
    hmac_update(hmac_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s\n", SIGNATURE_ALGORITHM);
#endif
    hmac_update(hmac_ctx, (const u_char *)datebuf, strlen(datebuf));
    hmac_update(hmac_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s\n", datebuf);
#endif
    hmac_update(hmac_ctx, (const u_char *)datebuf, 8);
    hmac_update(hmac_ctx, (const u_char *)"/", 1);
    hmac_update(hmac_ctx, (const u_char *)config->region, strlen(config->region));
    hmac_update(hmac_ctx, (const u_char *)"/", 1);
    hmac_update(hmac_ctx, (const u_char *)S3_SERVICE_NAME, strlen(S3_SERVICE_NAME));
    hmac_update(hmac_ctx, (const u_char *)"/", 1);
    hmac_update(hmac_ctx, (const u_char *)SIGNATURE_TERMINATOR, strlen(SIGNATURE_TERMINATOR));
    hmac_update(hmac_ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%.8s/%s/%s/%s\n",
      datebuf, config->region, S3_SERVICE_NAME, SIGNATURE_TERMINATOR);
#endif
    hmac_update(hmac_ctx, (const u_char *)creq_hash_buf, strlen(creq_hash_buf));
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sizeof(sigbuf) - strlen(sigbuf), "%s", creq_hash_buf);
#endif
    hmac_final(hmac_ctx, hmac_result);
    http_io_prhex(hmac_buf, hmac_result, sizeof(hmac_result));

#if DEBUG_AUTHENTICATION
    (*config->log)(LOG_DEBUG, "auth: key to sign:\n%s", sigbuf);
    (*config->log)(LOG_DEBUG, "auth: signature hmac = %s", hmac_buf);
#endif

/****** Add Authorization Header ******/

    http_io_add_header(priv, io, "%s: %s Credential=%s/%.8s/%s/%s/%s, SignedHeaders=%s, Signature=%s",
      AUTH_HEADER, SIGNATURE_ALGORITHM, access_id, datebuf, config->region, S3_SERVICE_NAME, SIGNATURE_TERMINATOR,
      header_names, hmac_buf);

    // Done
    r = 0;

fail:
    // Clean up
    if (sorted_hdrs != NULL)
        free(sorted_hdrs);
    free(header_names);
    EVP_MD_CTX_free(hash_ctx);
    hmac_free(hmac_ctx);
    return r;
}

/*
 * Reformat a query string for digest, adding any missing equals signs.
 * NOTE: This assumes that the query string parameters are already sorted.
 * Caller must free the result.
 */
static char *
canonicalize_query_string(const char *query, size_t qlen)
{
    char *buf;
    char *bp;
    size_t i;
    int saweq;

    if ((buf = malloc(qlen * 2 + 1)) == NULL)
        return NULL;
    bp = buf;
    saweq = 0;
    for (i = 0; i < qlen; i++) {
        char c = query[i];
        switch (c) {
        case '&':
            if (!saweq)
                *bp++ = '=';
            saweq = 0;
            break;
        case '=':
            if (!saweq)
                saweq = 1;
            break;
        default:
            break;
        }
        *bp++ = c;
    }
    if (qlen > 0 && !saweq)
        *bp++ = '=';
    *bp++ = '\0';
    return buf;
}

/*
 * Add data to digest, but in URL-encoded form.
 */
static void
digest_url_encoded(EVP_MD_CTX* hash_ctx, const char *data, size_t len, int encode_slash)
{
    char buf[len * 3 + 1];

    len = url_encode(data, len, buf, sizeof(buf), encode_slash);
    EVP_DigestUpdate(hash_ctx, (const u_char *)buf, len);
}

/*
 * URL-encode the given input.
 *
 * Aborts if buffer is not big enough.
 */
static size_t
url_encode(const char *src, size_t len, char *dst, int buflen, int encode_slash)
{
    char *const dst_base = dst;
    int empty = 1;
    size_t elen;

    while (len-- > 0) {
        const char ch = *src++;
        if (isalnum(ch) || ch == '_' || ch == '-' || ch == '~' || ch == '.' || (ch == '/' && !encode_slash))
            elen = snvprintf(dst, buflen, "%c", ch);
        else
            elen = snvprintf(dst, buflen, "%%%02X", (int)ch & 0xff);
        dst += elen;
        buflen -= elen;
        empty = 0;
    }
    if (empty)
        snvprintf(dst, buflen, "%s", "");   // this catches the oddball case where len == buflen == 0
    return dst - dst_base;
}

/*
 * Create URL for a block, and return pointer to the URL's URI path.
 */
static void
http_io_get_block_url(char *buf, size_t bufsiz, struct http_io_conf *config, s3b_block_t block_num)
{
    char block_hash_buf[S3B_BLOCK_NUM_DIGITS + strlen(BLOCK_HASH_PREFIX_SEPARATOR) + 1];

    http_io_format_block_hash(config->blockHashPrefix, block_hash_buf, sizeof(block_hash_buf), block_num);
    if (config->vhost) {
        snvprintf(buf, bufsiz, "%s%s%s%0*jx", config->baseURL,
          config->prefix, block_hash_buf, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
    } else {
        snvprintf(buf, bufsiz, "%s%s/%s%s%0*jx", config->baseURL,
          config->bucket, config->prefix, block_hash_buf, S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
    }
}

/*
 * Create URL for the mount token file, and return pointer to the URL's path not including any "/bucket" prefix.
 */
static void
http_io_get_mount_token_file_url(char *buf, size_t bufsiz, struct http_io_conf *config)
{
    if (config->vhost)
        snvprintf(buf, bufsiz, "%s%s%s", config->baseURL, config->prefix, MOUNT_TOKEN_FILE);
    else
        snvprintf(buf, bufsiz, "%s%s/%s%s", config->baseURL, config->bucket, config->prefix, MOUNT_TOKEN_FILE);
}

static int
http_io_add_header(struct http_io_private *priv, struct http_io *io, const char *fmt, ...)
{
    struct curl_slist *new_list;
    va_list args;
    char *buf;
    int r = 0;

    va_start(args, fmt);
    if (vasprintf(&buf, fmt, args) == -1) {
        r = errno;
        (*priv->config->log)(LOG_ERR, "%s: vasprintf() failed: %s", "http_io_add_header", strerror(r));
    } else {
        if ((new_list = curl_slist_append(io->headers, buf)) != NULL)
            io->headers = new_list;
        else
            r = ENOMEM;
        free(buf);
    }
    va_end(args);
    return r;
}

static CURL *
http_io_acquire_curl(struct http_io_private *priv, struct http_io *io)
{
    struct http_io_conf *const config = priv->config;
    struct curl_holder *holder;
    CURL *curl;

    // Get a CURL instance
    pthread_mutex_lock(&priv->mutex);
    if ((holder = LIST_FIRST(&priv->curls)) != NULL) {
        curl = holder->curl;
        LIST_REMOVE(holder, link);
        priv->stats.curl_handles_reused++;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        free(holder);
        curl_easy_reset(curl);
    } else {
        priv->stats.curl_handles_created++;             // optimistic
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        if ((curl = curl_easy_init()) == NULL) {
            pthread_mutex_lock(&priv->mutex);
            priv->stats.curl_handles_created--;         // undo optimistic
            priv->stats.curl_other_error++;
            CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
            (*config->log)(LOG_ERR, "curl_easy_init() failed");
            return NULL;
        }
    }

    // Set various options
    if (!http_io_curl_setopt_ptr(priv, curl, CURLOPT_URL, io->url)
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_FOLLOWLOCATION, 1)
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_NOSIGNAL, 1)
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_TCP_KEEPALIVE, 1)
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_TCP_KEEPIDLE, TCP_KEEP_ALIVE_IDLE)
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_TCP_KEEPINTVL, TCP_KEEP_ALIVE_INTERVAL)
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_TIMEOUT, config->timeout)
      || !http_io_curl_setopt_long(priv, curl, CURLOPT_NOPROGRESS, 1)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_USERAGENT, config->user_agent)
      || !http_io_curl_setopt_ptr(priv, curl, CURLOPT_SOCKOPTFUNCTION, http_io_sockopt_callback))
        goto optfail;
    if (config->max_speed[HTTP_UPLOAD] != 0
      && !http_io_curl_setopt_off(priv, curl, CURLOPT_MAX_SEND_SPEED_LARGE, (curl_off_t)(config->max_speed[HTTP_UPLOAD] / 8)))
        goto optfail;
    if (config->max_speed[HTTP_DOWNLOAD] != 0
      && !http_io_curl_setopt_off(priv, curl, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)(config->max_speed[HTTP_DOWNLOAD] / 8)))
        goto optfail;
    if (strncmp(io->url, "https", 5) == 0) {
        if (config->insecure
          && !http_io_curl_setopt_long(priv, curl, CURLOPT_SSL_VERIFYPEER, 0))
            goto optfail;
        if (config->cacert != NULL
          && !http_io_curl_setopt_ptr(priv, curl, CURLOPT_CAINFO, config->cacert))
            goto optfail;
    }
    if (config->debug_http
      && !http_io_curl_setopt_long(priv, curl, CURLOPT_VERBOSE, 1))
        goto optfail;
    if (config->http_11
      && !http_io_curl_setopt_long(priv, curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_1_1))
        goto optfail;
    if (strcmp(io->method, HTTP_POST) != 0
      && !http_io_curl_setopt_long(priv, curl, CURLOPT_POST, 0))
        goto optfail;
    return curl;

optfail:
    http_io_release_curl(priv, &curl, 0);
    return NULL;
}

static size_t
http_io_curl_reader(const void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct http_io *const io = (struct http_io *)stream;
    struct http_io_bufs *const bufs = &io->bufs;
    size_t total = size * nmemb;

    // Check for error payload
    if (http_io_reader_error_check(io, ptr, total))
        return total;

    // Copy payload bytes into read buffer
    if (total > bufs->rdremain)     // should never happen
        total = bufs->rdremain;
    memcpy(bufs->rddata, ptr, total);
    bufs->rddata += total;
    bufs->rdremain -= total;
    return total;
}

static size_t
http_io_curl_writer(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct http_io *const io = (struct http_io *)stream;
    struct http_io_bufs *const bufs = &io->bufs;
    size_t total = size * nmemb;

    // Check for canceled write
    if (io->check_cancel != NULL && (*io->check_cancel)(io->check_cancel_arg, io->block_num) != 0)
        return CURL_READFUNC_ABORT;

    // Copy out data
    if (total > bufs->wrremain)     // should never happen
        total = bufs->wrremain;
    memcpy(ptr, bufs->wrdata, total);
    bufs->wrdata += total;
    bufs->wrremain -= total;
    return total;
}

static size_t
http_io_curl_header(void *ptr, size_t size, size_t nmemb, void *stream)
{
    struct http_io *const io = (struct http_io *)stream;
    const size_t total = size * nmemb;
    char hashbuf[64];
    char buf[1024];
    u_int mtoken;

    // Null-terminate header
    if (total > sizeof(buf) - 1)
        return total;
    memcpy(buf, ptr, total);
    buf[total] = '\0';

    // Check for interesting headers
    http_io_parse_header(io, buf, FILE_SIZE_HEADER, 1, "%ju", &io->file_size);
    http_io_parse_header(io, buf, BLOCK_SIZE_HEADER, 1, "%u", &io->block_size);
    if (http_io_parse_header(io, buf, MOUNT_TOKEN_HEADER, 1, "%x", &mtoken))
        io->mount_token = (int32_t)mtoken;

    // ETag header
    if (http_io_parse_header(io, buf, ETAG_HEADER, 1, "\"%32c\"", hashbuf))
        http_io_parse_hex(hashbuf, io->etag, MD5_DIGEST_LENGTH);

    // "x-amz-meta-s3backer-hmac" header
    if (http_io_parse_header(io, buf, HMAC_HEADER, 1, "\"%40c\"", hashbuf))
        http_io_parse_hex(hashbuf, io->hmac, SHA_DIGEST_LENGTH);

    // Content encoding(s)
    if (strncasecmp(buf, CONTENT_ENCODING_HEADER ":", sizeof(CONTENT_ENCODING_HEADER)) == 0) {
        size_t celen;
        char *state;
        char *s;

        *io->content_encoding = '\0';
        for (s = strtok_r(buf + sizeof(CONTENT_ENCODING_HEADER), WHITESPACE ",", &state);
          s != NULL; s = strtok_r(NULL, WHITESPACE ",", &state)) {
            celen = strlen(io->content_encoding);
            snvprintf(io->content_encoding + celen, sizeof(io->content_encoding) - celen, "%s%s", celen > 0 ? "," : "", s);
        }
    }

    // Done
    return total;
}

// Reset fields that contain HTTP response information populated by http_io_curl_header()
static void
http_io_curl_header_reset(struct http_io *const io)
{
    io->file_size = 0;
    io->block_size = 0;
    io->mount_token = 0;
    memset(io->etag, 0, sizeof(io->etag));
    memset(io->hmac, 0, sizeof(io->hmac));
    memset(io->content_encoding, 0, sizeof(io->content_encoding));
}

static int
http_io_curl_setopt_long(struct http_io_private *priv, CURL *curl, CURLoption option, long value)
{
    CURLcode curl_code;

    assert(option / 10000 == CURLOPTTYPE_LONG / 10000);
    if ((curl_code = curl_easy_setopt(curl, option, value)) != CURLE_OK) {
        (*priv->config->log)(LOG_ERR, "curl_easy_setopt(%d): %s", (int)option, curl_easy_strerror(curl_code));
        return 0;
    }
    return 1;
}

static int
http_io_curl_setopt_ptr(struct http_io_private *priv, CURL *curl, CURLoption option, const void *ptr)
{
    CURLcode curl_code;

    assert(option / 10000 == CURLOPTTYPE_OBJECTPOINT / 10000
      || option / 10000 == CURLOPTTYPE_FUNCTIONPOINT / 10000);
    if ((curl_code = curl_easy_setopt(curl, option, ptr)) != CURLE_OK) {
        (*priv->config->log)(LOG_ERR, "curl_easy_setopt(%d): %s", (int)option, curl_easy_strerror(curl_code));
        return 0;
    }
    return 1;
}

static int
http_io_curl_setopt_off(struct http_io_private *priv, CURL *curl, CURLoption option, curl_off_t offset)
{
    CURLcode curl_code;

    assert(option / 10000 == CURLOPTTYPE_OFF_T / 10000);
    if ((curl_code = curl_easy_setopt(curl, option, offset)) != CURLE_OK) {
        (*priv->config->log)(LOG_ERR, "curl_easy_setopt(%d): %s", (int)option, curl_easy_strerror(curl_code));
        return 0;
    }
    return 1;
}

static int
http_io_sockopt_callback(void *cookie, curl_socket_t fd, curlsocktype purpose)
{
    (void)fcntl(fd, F_SETFD, FD_CLOEXEC);
    return CURL_SOCKOPT_OK;
}

// Re-use curl handles unless there's a worry that the connection might be broken
static int
http_io_safe_to_cache_curl_handle(CURLcode curl_code, long http_code)
{
    switch (curl_code) {
    case CURLE_HTTP_RETURNED_ERROR:         // we got a normal HTTP error back
        return http_code < 500;             // don't cache after server errors, just to be safe
    default:                                // something weird happened, definitely don't cache
        return 0;
    }
}

static void
http_io_release_curl(struct http_io_private *priv, CURL **curlp, int may_cache)
{
    struct http_io_conf *const config = priv->config;
    struct curl_holder *holder;
    CURL *const curl = *curlp;

    *curlp = NULL;
    assert(curl != NULL);
    if (config->no_curl_cache || !may_cache) {
        curl_easy_cleanup(curl);
        return;
    }
    if ((holder = calloc(1, sizeof(*holder))) == NULL) {
        curl_easy_cleanup(curl);
        pthread_mutex_lock(&priv->mutex);
        priv->stats.out_of_memory_errors++;
        CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
        return;
    }
    holder->curl = curl;
    pthread_mutex_lock(&priv->mutex);
    LIST_INSERT_HEAD(&priv->curls, holder, link);
    CHECK_RETURN(pthread_mutex_unlock(&priv->mutex));
}

static int
http_io_reader_error_check(struct http_io *const io, const void *ptr, size_t len)
{
    struct http_io_conf *const config = io->config;
    char *new_error_payload;

    // Get HTTP status, if not done already
    if (io->http_status == 0
      && (curl_easy_getinfo(io->curl, CURLINFO_RESPONSE_CODE, &io->http_status) != 0 || io->http_status == 0))
        io->http_status = 999;                          // this should never happen

    // If status is not an error status, proceed normally
    if (io->http_status < HTTP_STATUS_ERROR_MINIMUM)
        return 0;

    // If no debug flag was given, just discard error payloads
    if (!config->debug_http)
        return 1;

    // Impose limit on how much error payload we'll remember
    if (io->error_payload_len + len > MAX_DEBUG_PAYLOAD_SIZE)
        len = MAX_DEBUG_PAYLOAD_SIZE - io->error_payload_len;

    // Capture the error payload in the error buffer
    if ((new_error_payload = realloc(io->error_payload, io->error_payload_len + len)) == NULL)
        (*io->config->log)(LOG_ERR, "realloc: %s", strerror(errno));
    else {
        io->error_payload = new_error_payload;
        memcpy(io->error_payload + io->error_payload_len, ptr, len);
        io->error_payload_len += len;
    }

    // Indicate to caller we're handling the payload
    return 1;
}

static void
http_io_free_error_payload(struct http_io *const io)
{
    free(io->error_payload);
    io->error_payload = NULL;
    io->error_payload_len = 0;
}

static void
http_io_log_error_payload(struct http_io *const io)
{
    struct http_io_conf *const config = io->config;

    if (config->debug_http && io->error_payload_len > 0) {
        (*config->log)(LOG_DEBUG, "HTTP %d status response payload:\n%.*s",
          (int)io->http_status, (int)io->error_payload_len, io->error_payload);
    }
}

static void
http_io_openssl_locker(int mode, int i, const char *file, int line)
{
    if ((mode & CRYPTO_LOCK) != 0)
        pthread_mutex_lock(&openssl_locks[i]);
    else
        CHECK_RETURN(pthread_mutex_unlock(&openssl_locks[i]));
}

static u_long
http_io_openssl_ider(void)
{
    return (u_long)pthread_self();
}

static void
http_io_base64_encode(char *buf, size_t bufsiz, const void *data, size_t len)
{
    BUF_MEM *bptr;
    BIO* bmem;
    BIO* b64;

    b64 = BIO_new(BIO_f_base64());
    assert(b64 != NULL);
    bmem = BIO_new(BIO_s_mem());
    assert(bmem != NULL);
    b64 = BIO_push(b64, bmem);
    BIO_write(b64, data, len);
    (void)BIO_flush(b64);
    BIO_get_mem_ptr(b64, &bptr);
    snvprintf(buf, bufsiz, "%.*s", (int)bptr->length - 1, (char *)bptr->data);
    BIO_free_all(b64);
}

static void
http_io_init_io(struct http_io_private *priv, struct http_io *io, const char *method, const char *url)
{
    memset(io, 0, sizeof(*io));
    io->config = priv->config;
    io->method = method;
    io->url = url;
}

/*
 * Encrypt or decrypt one block
 */
static u_int
http_io_crypt(struct http_io_private *priv, s3b_block_t block_num, int enc, const u_char *src, u_int len, u_char *dest, u_int dmax)
{
    u_char ivec[EVP_MAX_IV_LENGTH];
    EVP_CIPHER_CTX* ctx;
    u_int total_len;
    char blockbuf[EVP_MAX_IV_LENGTH];
    int clen;
    int r;

#ifdef NDEBUG
    // Avoid unused variable warning
    (void)r;
#endif

    // Sanity check
    assert(EVP_MAX_IV_LENGTH >= MD5_DIGEST_LENGTH);

    // Initialize cipher context
    ctx = EVP_CIPHER_CTX_new();
    assert(ctx != NULL);
    EVP_CIPHER_CTX_init(ctx);

    // Generate initialization vector by encrypting the block number using previously generated IV
    memset(blockbuf, 0, sizeof(blockbuf));
    snvprintf(blockbuf, sizeof(blockbuf), "%0*jx", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);

    // Initialize cipher for IV generation
    r = EVP_EncryptInit_ex(ctx, priv->cipher, NULL, priv->ivkey, priv->ivkey);
    assert(r == 1);
    EVP_CIPHER_CTX_set_padding(ctx, 0);

    // Encrypt block number to get IV for bulk encryption
    r = EVP_EncryptUpdate(ctx, ivec, &clen, (const u_char *)blockbuf, EVP_CIPHER_CTX_block_size(ctx));
    assert(r == 1 && clen == EVP_CIPHER_CTX_block_size(ctx));
    r = EVP_EncryptFinal_ex(ctx, NULL, &clen);
    assert(r == 1 && clen == 0);

    // Re-initialize cipher for bulk data encryption
    assert(EVP_CIPHER_CTX_block_size(ctx) == EVP_CIPHER_CTX_iv_length(ctx));
    r = EVP_CipherInit_ex(ctx, priv->cipher, NULL, priv->key, ivec, enc);
    assert(r == 1);
    EVP_CIPHER_CTX_set_padding(ctx, 1);

    // Encrypt/decrypt
    r = EVP_CipherUpdate(ctx, dest, &clen, src, (int)len);
    assert(r == 1 && clen >= 0);
    total_len = (u_int)clen;
    r = EVP_CipherFinal_ex(ctx, dest + total_len, &clen);
    assert(r == 1 && clen >= 0);
    total_len += (u_int)clen;

    // Encryption debug
#if DEBUG_ENCRYPTION
{
    struct http_io_conf *const config = priv->config;
    char ivecbuf[sizeof(ivec) * 2 + 1];
    http_io_prhex(ivecbuf, ivec, sizeof(ivec));
    (*config->log)(LOG_DEBUG, "%sCRYPT: block=%s ivec=0x%s len: %d -> %d", (enc ? "EN" : "DE"), blockbuf, ivecbuf, len, total_len);
}
#endif

    // Sanity check
    if (total_len > dmax) {
        (*priv->config->log)(LOG_ERR, "encryption buffer overflow! %u > %u", total_len, dmax);
        abort();
    }

    // Done
    EVP_CIPHER_CTX_free(ctx);
    return total_len;
}

static void
http_io_authsig(struct http_io_private *priv, s3b_block_t block_num, const u_char *src, u_int len, u_char *hmac)
{
    const char *const ciphername = EVP_CIPHER_name(priv->cipher);
    char blockbuf[64];
    struct hmac_ctx* ctx;

    // Sign the block number, the name of the encryption algorithm, and the block data
    snvprintf(blockbuf, sizeof(blockbuf), "%0*jx", S3B_BLOCK_NUM_DIGITS, (uintmax_t)block_num);
    ctx = hmac_new_sha1(priv->hmac, priv->key, priv->keylen);
    assert(ctx != NULL);
    hmac_update(ctx, (const u_char *)blockbuf, strlen(blockbuf));
    hmac_update(ctx, (const u_char *)ciphername, strlen(ciphername));
    hmac_update(ctx, (const u_char *)src, len);
    hmac_final(ctx, (u_char *)hmac);
    hmac_free(ctx);
}

static void
update_hmac_from_header(struct hmac_ctx *const ctx, struct http_io *const io,
  const char *name, int value_only, char *sigbuf, size_t sigbuflen)
{
    const struct curl_slist *header;
    const char *colon;
    const char *value;
    size_t name_len;

    // Find and add header
    name_len = (colon = strchr(name, ':')) != NULL ? colon - name : strlen(name);
    for (header = io->headers; header != NULL; header = header->next) {
        if (strncasecmp(header->data, name, name_len) == 0 && header->data[name_len] == ':') {
            if (!value_only) {
                hmac_update(ctx, (const u_char *)header->data, name_len + 1);
#if DEBUG_AUTHENTICATION
                snvprintf(sigbuf + strlen(sigbuf), sigbuflen - strlen(sigbuf), "%.*s", (int)name_len + 1, header->data);
#endif
            }
            for (value = header->data + name_len + 1; isspace(*value); value++)
                ;
            hmac_update(ctx, (const u_char *)value, strlen(value));
#if DEBUG_AUTHENTICATION
            snvprintf(sigbuf + strlen(sigbuf), sigbuflen - strlen(sigbuf), "%s", value);
#endif
            break;
        }
    }

    // Add newline whether or not header was found
    hmac_update(ctx, (const u_char *)"\n", 1);
#if DEBUG_AUTHENTICATION
    snvprintf(sigbuf + strlen(sigbuf), sigbuflen - strlen(sigbuf), "\n");
#endif
}

/*
 * Parse exactly "nbytes" contiguous 2-digit hex bytes.
 * On failure, zero out the buffer and return -1.
 */
static int
http_io_parse_hex(const char *str, u_char *buf, u_int nbytes)
{
    int i;

    // Parse hex string
    for (i = 0; i < nbytes; i++) {
        int byte;
        int j;

        for (byte = j = 0; j < 2; j++) {
            const char ch = str[2 * i + j];

            if (!isxdigit(ch)) {
                memset(buf, 0, nbytes);
                return -1;
            }
            byte <<= 4;
            byte |= ch <= '9' ? ch - '0' : tolower(ch) - 'a' + 10;
        }
        buf[i] = byte;
    }

    // Done
    return 0;
}

static void
http_io_prhex(char *buf, const u_char *data, size_t len)
{
    static const char *hexdig = "0123456789abcdef";
    int i;

    for (i = 0; i < len; i++) {
        buf[i * 2 + 0] = hexdig[data[i] >> 4];
        buf[i * 2 + 1] = hexdig[data[i] & 0x0f];
    }
    buf[i * 2] = '\0';
}

/*
 * Sort two header strings of the form "Foo: Bar" case-insensitively by header name (only).
 */
static int
http_io_header_name_sort(const void *const ptr1, const void *const ptr2)
{
    const char *const str1 = *(const char *const *)ptr1;
    const char *const str2 = *(const char *const *)ptr2;
    size_t i;

    for (i = 0; str1[i] != '\0' || str2[i] != '\0'; i++) {
        const int ch1 = str1[i] == ':' ? '\0' : tolower(str1[i]);
        const int ch2 = str2[i] == ':' ? '\0' : tolower(str2[i]);
        const int diff = ch1 - ch2;

        if (diff != 0)
            return diff;
    }
    return 0;
}

static int
http_io_parse_header(struct http_io *const io, const char *const input,
    const char *const header, int num_conversions, const char *const fmt, ...)
{
    va_list args;
    size_t offset;
    int r = 0;

    // Initialize
    va_start(args, fmt);

    // Match header (case-insensitively) followed by ':' and optional whitespace
    offset = strlen(header);
    if (strncasecmp(input, header, offset) != 0 || input[offset++] != ':')
        goto fail;
    while (isspace(input[offset]))
        offset++;

    // Parse header value; log an error if it's bogus
    if (vsscanf(input + offset, fmt, args) != num_conversions) {
        (*io->config->log)(LOG_ERR, "%s %s: rec'd malformed response header: %s: %s", io->method, io->url, header, input + offset);
        goto fail;
    }

    // Success
    r = 1;

fail:
    // Done
    va_end(args);
    return r;
}
