/**
 * Local Proxy & Cookie Pool cached from database 
 * with periodic refresh.
 */
const Sequelize = require('sequelize');
const Promise = require("bluebird");


class LocalPool{

    static _rand(limit){
       return  Math.round(Math.random() * limit);
    }

    constructor(dbOptions, proxyQry, cookieQry){
        this.proxies = [];
        this.cookies = [];
        this.proxyCounter = 0;
        this.cookieCounter = 0;
        this.refreshMs = 0;
        this.scheduler = null;

       this.options = dbOptions || {
               database: 'proxyIP1',
               user: 'sa',
               password: 'Datamining2014',
               host: 'XXXXXXXXX',
               port : 49281,
               dialect: 'mssql',

               pool: {
                   max: 2,
                   min: 0,
                   idle: 10000
               },

           };
       this.sqlProxyQry = proxyQry || "SELECT TOP 1000 [ipAddress],*  FROM [ProxyIP1].[dbo].[proxyIp] where status='1';";
        this.sqlCookieQry = cookieQry || "SELECT *  FROM [ProxyIP1].[dbo].[cookies] where status=1 and isUse=0;";


    }

    _refreshData(){

        return new Promise((resolve,reject) => {
            let sequelize;
            if ((!this.proxies.length) || (!this.cookies.length)) {
                sequelize = new Sequelize(this.options.database,this.options.user,this.options.password,this.options);
                Promise.all([
                    sequelize.query(this.sqlProxyQry, {type: sequelize.QueryTypes.SELECT}),
                    sequelize.query(this.sqlCookieQry, {type: sequelize.QueryTypes.SELECT})
                ]).spread( (proxies, cookies) => {
                    this.proxies = proxies;
                    this.proxyCounter = LocalPool._rand(this.proxies.length -1);
                    this.cookies = cookies;
                    this.cookieCounter = LocalPool._rand(this.cookies.length -1);
                    resolve('populated');
                }, function (e) {
                    console.log(e);
                    reject(e);
                }).finally(() => {
                    return sequelize.close();
                })
            } else {
                  resolve("populated");
            }
        });
    }
    getData(){
        return this._refreshData().then(() =>{
            return {proxy: this.proxies[this.proxyCounter++%this.proxies.length],
                cookies: this.cookies[this.cookieCounter++%this.cookies.length]};
        })
    }
    getProxy(){
        return this._refreshData().then(() =>{
            return this.proxies[this.proxyCounter++%this.proxies.length];
        })
    }
    getCookie(){
        return this._refreshData().then(() =>{
            return this.cookies[this.cookieCounter++%this.cookies.length];
        })
    }
    setAutoRefresh(refresh){
        if(refreshRate){
            if(!isNaN(parseFloat(refreshRate)) && isFinite(refreshRate)){
                if(refreshRate > 0) {
                    this.refreshMs = refreshRate * 1000 * 60;
                }else{
                    this.refreshMs = 60 * 1000;
                }
            }else{
                return new Error("Invalid refresh rate");
            }
        }
        console.log(`local cookie & proxy pool refreshed; refresh cycle : ${this.refreshMs}ms`);
        if(this.refreshMs) {
            this.scheduler = setInterval(() => {
                this._refreshData().then(() => {
                    console.log(`local cookie & proxy pool refreshed; refresh cycle : ${this.refreshMs}ms`);
                }).catch((e) => {
                    console.log("unable to refresh local cookie & proxy pool");
                });
            });
        }
    }

    clearAutoRefresh(){
        if(this.scheduler){
            try {
                clearInterval(this.scheduler);
            }catch(e){}

        }
        console.log('Local pool for cookie & proxy: autorefresh cleared');
    }

}

module.exports = LocalPool;


