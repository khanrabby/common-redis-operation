const RedisClustr = require('redis-clustr');

const redis = require('redis');

const redisHostCluster = [
    {host: 'xxxxx.xxxx.xxxx',port: 6379}, 
    {host: 'xxxxx.xxxx.xxxx',port: 6379},
    {host: 'xxxxx.xxxx.xxxx',port: 6379},
    {host: 'xxxxx.xxxx.xxxx',port: 6379}
  ]

/** CONNECT TO REDIS IN CLUSTER MODE */
const client = new RedisClustr({
    servers: redisHostCluster
});

/** CONNECT TO REDIS IN NON CLUSTER MODE */
//const client = redis.createClient(port, host);

const { promisify } = require("util");

const hget = promisify(client.hget).bind(client);
const hset = promisify(client.hset).bind(client);
const exist = promisify(client.exists).bind(client);
const hexist = promisify(client.hexists).bind(client);
const hdel = promisify(client.hdel).bind(client);
const del = promisify(client.del).bind(client);
const hkeys = promisify(client.hkeys).bind(client);
const hincrby  = promisify(client.hincrby).bind(client);
const keys = promisify(client.keys).bind(client);
//const hgetall = promisify(client.hgetall).bind(client);

module.exports = {
    insertDataToRedis : async(keyname,fieldName,data) =>{

        let setInRedis = false; 
    
        await hset(keyname, fieldName, JSON.stringify(data))
            .then(async () => {
                setInRedis = true; 
            })
            .catch((err) => {
                setInRedis = false; 
            });
        
        return setInRedis
    },

    incrementRedisData : async(keyname,fieldName,value) =>{
        let setInRedis = false; 
    
        await hincrby(keyname, fieldName, JSON.stringify(value))
            .then(async () => {
                setInRedis = true; 
            })
            .catch((err) => {
                setInRedis = false; 
            });
        
        return setInRedis
    },
    
    pullDataFromRedis : async (keyname,fieldName) =>{
        let data = ""; 
        await hget(keyname, fieldName)
        .then(async(response)=>{
            data = JSON.parse(response);
        })
        .catch(async(error)=>{
            data = null;
        })
        
        return data;
    },
    
    deleteFromRedis : async(keyname,fieldName) =>{
        let removeFromRedis = false; 
    
        await hdel(keyname, fieldName)
            .then(() => {
                removeFromRedis = true;
            })
            .catch(() => {
                removeFromRedis = false; 
            });
        
        return removeFromRedis; 
    },

    deleteKeyFromRedis : async(keyname) =>{
        let removeFromRedis = false; 
    
        await del(keyname)
            .then(() => {
                removeFromRedis = true;
            })
            .catch(() => {
                removeFromRedis = false; 
            });
        
        return removeFromRedis; 
    },
    
    isRedisKeyExist : async(keyName) =>{
        let countryKeyExist = false;
        await exist(keyName)
        .then(async(rep)=>{
            if(rep === 1){
                countryKeyExist = true; 
            }
        })
        .catch(async(error)=>{
            
        });
    
        return countryKeyExist;
    },

    isRedisFiledExists : async(keyName,fieldName) =>{
        let countryKeyExist = false;
        await hexist(keyName,fieldName)
        .then(async(rep)=>{
            if(rep === 1){
                countryKeyExist = true; 
            }
        })
        .catch(async(error)=>{
        });
    
        return countryKeyExist;
    },

    getAllRedisFields: async (keyname) => {
        let data;
        await hkeys(keyname)
            .then((keyData) => {
                data = keyData;
            })
            .catch(async (error) => {
                data = [];
            })

        return data;
    },

    getAllKeyData: async (keyname) => {
        const idArray = await module.exports.getAllRedisFields(keyname);
        let dataArray = [];
        for (let i = 0; i < idArray.length; i++) {
            let detailsData = await module.exports.pullDataFromRedis(keyname, idArray[i]);
            dataArray.push(detailsData);
        }

        return dataArray;
    },

    /** GET ALL KEYS ACCORDING TO GIVEN PATTERN FROM A REDIS CLUSTER */
    getKeysByPattern: async (pattern) => {
        let result = [];

        const parentPromises = [];

        const getKeys = async (redisClient, pattern) => {
            return new Promise(async (resolve, reject) => {
                const keys = redisClient.keys(pattern, function (err, keys) {

                    if (err) {
                        resolve([])
                    }
                    else {
                        resolve(keys);
                    }
                })
            });
        }

        for (i = 0; i < rabbitRedisHostCluster.length; i++) {
            const redisClient = redis.createClient(rabbitRedisHostCluster[i].port, rabbitRedisHostCluster[i].host);
            parentPromises.push(getKeys(redisClient, pattern));
        }

        result = await Promise.all(parentPromises)
            .then(async (results) => {
                let returnData = [];
                for (let i = 0; i < results.length; i++) {
                    returnData = [...returnData, ...results[i]]
                }
                return returnData;
            })
            .catch(async (error) => {
                return [];
            })

        return result;
    }
}

