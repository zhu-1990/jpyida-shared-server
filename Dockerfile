FROM node:18-alpine

WORKDIR /app

# 先装依赖（利用层缓存）
COPY package.json ./
RUN npm install --omit=dev

# 复制服务代码
COPY server.js ./

# 持久化数据卷挂载点（Koyeb 卷挂到 /data）
ENV DATA_DIR=/data
ENV PORT=3000
ENV NODE_ENV=production

EXPOSE 3000

CMD ["node", "server.js"]
