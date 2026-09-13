import express from 'express';
import fs from 'fs-extra';
import path from 'path';
import sharp from 'sharp';
import { exec } from 'child_process';
import mongoose from 'mongoose';
import moment from 'moment-timezone';
import https from 'https';
import axios from 'axios';
import dotenv from 'dotenv';
import yts from 'yt-search';
dotenv.config();

import {
    default as makeWASocket,
    useMultiFileAuthState,
    delay,
    Browsers,
    fetchLatestBaileysVersion,
    downloadContentFromMessage,
    jidNormalizedUser,
    isPnUser
} from '@whiskeysockets/baileys';

export const router = express.Router();
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

const insecureAgent = new https.Agent({
    rejectUnauthorized: false
});
const config = {
    AUTO_RECORDING: 'false',
    AUTO_TYPING: 'false',
    AUTO_REACT: 'false',
    READ_CMD: 'false',
    API_MAIN_URL: 'https://api-siteh-22e22e4cb068.herokuapp.com',
    API_MAIN_URL2:'https://api.laksidu.site',
    API_CINESUBZ_URL:'https://api-siteh-22e22e4cb068.herokuapp.com',
    API_MOVIE_URL: 'https://api-siteh-22e22e4cb068.herokuapp.com',
    API_KEY:'lakiya_2f3b6c382d1236ad7a08d56331fb679935d51dfc846df2c254093fd1fff9494e',
    BOT_IMAGE:'https://cdn.phototourl.com/free/2026-09-01-c9fad274-7d07-49ea-9ed1-34832687d820.jpg',
    BOT_FOOTER:"ꜱʜᴀɢɢY ✘ 〽️ᴏᴠɪᴇ Bᴏᴛ ᴠ1.1",
     MGROUP_LINK: 'https://chat.whatsapp.com/JpFSNrnqtnQIqdM0WlNds1',
    MOVIE_FOOTER:"​⏤͟͟͞͞★❮ SHAGGY 〽️OVIE ⏤͟͟͞͞★",
     MOVIE_CAPTION:"SHAGGY XMD",
    PREFIX: '.',
    OWNER_NUMBERS: ['94703830GGGG990'],
    BOT_NAME: "ꜱʜᴀɢɢY xᴍᴅ ꜰʀᴇᴇ ʙᴏᴛ",
    AIR_FOOTER: "Bᴏᴛ ᴠ2.0.0",
    MODE: 'public',
    MAX_RETRIES: 3
};
const activeSockets = new Map();
const socketCreationTime = new Map();
const SESSION_BASE_PATH = './session';
const NUMBER_LIST_PATH = './numbers.json';
const SessionSchema = new mongoose.Schema({
    number: { type: String, unique: true, required: true },
    creds: { type: Object, required: true },
    config: { type: Object },
    updatedAt: { type: Date, default: Date.now }
});
const Session = mongoose.model('Session', SessionSchema);

async function connectMongoDB() {
    try {
        const mongoUri = process.env.MONGO_URI;
        await mongoose.connect(mongoUri, {
            useNewUrlParser: true,
            useUnifiedTopology: true
        });
        console.log(`
╔══════════════════════════════════════╗
║  ✅ MongoDB Connected Successfully   ║
║  ⚡ System Status : ONLINE           ║
╚══════════════════════════════════════╝
`);
    } catch (error) {
        console.error('MongoDB connection failed:', error);
        process.exit(1);
    }
}
connectMongoDB();
if (!fs.existsSync(SESSION_BASE_PATH)) {
    fs.mkdirSync(SESSION_BASE_PATH, { recursive: true });
}

function initialize() {
    activeSockets.clear();
    socketCreationTime.clear();
    console.log('Cleared active sockets and creation times on startup');
}
async function autoReconnectOnStartup() {
    try {
        let numbers = [];
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
            console.log(`Loaded ${(numbers.length)} numbers from numbers.json`);
        } else {
            console.warn('No numbers.json found, checking MongoDB for sessions...');
        }

        const sessions = await Session.find({}, 'number').lean();
        const mongoNumbers = sessions.map(s => s.number);
        console.log(`Found ${mongoNumbers.length} numbers in MongoDB sessions`);

        numbers = [...new Set([...numbers, ...mongoNumbers])];
        if (numbers.length === 0) {
            console.log('No numbers found in numbers.json or MongoDB, skipping auto-reconnect');
            return;
        }

        console.log(`Attempting to reconnect ${numbers.length} sessions...`);
        for (const number of numbers) {
            if (activeSockets.has(number)) {
                console.log(`Number ${number} already connected, skipping`);
                continue;
            }
            const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
            try {
                await EmpirePair(number, mockRes);
                console.log(`Initiated reconnect for ${number}`);
            } catch (error) {
                console.error(`Failed to reconnect ${number}:`, error);
            }
            await delay(1000);
        }
    } catch (error) {
        console.error('Auto-reconnect on startup failed:', error);
    }
}

initialize();
setTimeout(autoReconnectOnStartup, 5000);
function formatMessage(title, content, footer) {
    return `*${title}*\n\n${content}\n\n> *${footer}*`;
}
function getSriLankaTimestamp() {
    return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss');
}
async function downloadContent(message) {
    if (!message) throw new Error('No message content');
    const buffer = await downloadContentFromMessage(message, 'buffer');
    return buffer;
}
async function streamToBuffer(stream) {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks);
}
async function setupCommandHandlers(socket, number) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    let sessionConfig = await loadUserConfig(sanitizedNumber);
    activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

    socket.ev.on('messages.upsert', async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message) return;

        let text = '';
        if (msg.message.conversation) {
            text = msg.message.conversation.trim();
        } else if (msg.message.extendedTextMessage?.text) {
            text = msg.message.extendedTextMessage.text.trim();
        } else if (msg.message.buttonsResponseMessage) {
            text = msg.message.buttonsResponseMessage.selectedButtonId;
        } else {
            return;
        }

        const userJid = jidNormalizedUser(socket.user.id);
        const from = msg.key.remoteJid;
        const sender = from;
        const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || msg.key.remoteJid);
        const senderNumber = (nowsender || '').split('@')[0];
        const developers = `${config.OWNER_NUMBERS}`;
        const botNumber = socket.user.id.split(':')[0];
        const isbot = botNumber.includes(senderNumber);
        const isOwner = isbot ? isbot : developers.includes(senderNumber);
        const isGroup = from.endsWith("@g.us");
        const isCmd = text.startsWith(sessionConfig.PREFIX || '!');

        if (!sessionConfig.MODE === 'public') return;
        if (!isOwner && sessionConfig.MODE === 'private') return;
        if (!isOwner && isGroup && sessionConfig.MODE === 'inbox') return;
        if (!isOwner && !isGroup && sessionConfig.MODE === 'groups') return;

        if (isCmd && sessionConfig.READ_CMD === 'true') {
            try {
                await socket.readMessages([msg.key]);
            } catch (error) {
               
            }
        }

        if (!isCmd) return;
        const parts = text.slice((sessionConfig.PREFIX || '!').length).trim().split(/\s+/);
        const command = parts[0].toLowerCase();
        const args = parts.slice(1);

        const groupMetadata = isGroup ? await socket.groupMetadata(msg.key.remoteJid) : {};
        const participants = groupMetadata.participants || [];
        const groupAdmins = participants.filter((p) => p.admin).map((p) => p.id);
        const isBotAdmins = groupAdmins.includes(socket.user.id);
        const isAdmins = groupAdmins.includes(sender);

        const reply = async (text, options = {}) => {
            await socket.sendMessage(msg.key.remoteJid, { text, ...options }, { quoted: msg });
        };

        try {
            switch (command) {
            case 'song':
    if (!args.length) {
        await socket.sendMessage(sender, {
            text: '❌ ERROR\n\n*Need YouTube URL or Song Title*'
        }, { quoted: msg });
        break;
    }

    const songQuery = args.join(' ');
    await socket.sendMessage(sender, { text: '🔍 Searching song...' });

    try {
        let data;
        if (songQuery.match(/(youtube\.com|youtu\.be)/)) {
            const match = songQuery.match(/(?:v=|\/)([0-9A-Za-z_-]{11})/);
            const videoId = match ? match[1] : null;

            if (!videoId) throw new Error('Invalid YouTube URL');

            const result = await yts({ videoId });
            data = result;
        } else {
            const result = await yts(songQuery);

            if (!result.videos || result.videos.length === 0) {
                await socket.sendMessage(sender, {
                    text: '❌ NO RESULTS\n\n*No results found for your query*'
                }, { quoted: msg });
                break;
            }

            data = result.videos[0];
        }

        if (!data) throw new Error('No results');

        const videoId = data.videoId;
        const desc = ` *ᴛɪᴛʟᴇ* : _${data.title || 'N/A'}_     

* ⏱️ 𝗗ᴜʀᴀᴛɪᴏɴ* ➟ _${data.timestamp || 'N/A'}_
* 👀 𝗩ɪᴇᴡꜱ* ➟ _${data.views?.toLocaleString() || 'N/A'}_
* 📅 𝗣ᴜʙʟɪꜱʜᴇᴅ* ➟ _${data.ago || 'N/A'}_
* 🎤 𝗖ʜᴀɴɴᴇʟ* ➟ _${data.author?.name || 'N/A'}_
*🔢 𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 👇*

*01 ᴅᴏᴡɴʟᴏᴀᴅ ᴀᴜᴅɪᴏ*
*02 ᴅᴏᴡɴʟᴏᴀᴅ ᴅᴏᴄᴜᴍᴇɴᴛ*
`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: data.thumbnail },
            caption: desc
        }, { quoted: msg });
        const listener = async (update) => {
            const mek = update.messages[0];
            if (!mek?.message) return;
            const ctx = mek.message.extendedTextMessage?.contextInfo;
            if (!ctx || ctx.stanzaId !== sentMsg.key.id) return;
            const text =
                mek.message.conversation ||
                mek.message.extendedTextMessage?.text;

            if (!['1', '2'].includes(text)) return;
            socket.ev.off('messages.upsert', listener);

            await socket.sendMessage(sender, { react: { text: '⬇️', key: mek.key } });

            try {
                 const apiUrl = `${config.API_MAIN_URL}/api/ytmp3?url=https://youtu.be/${videoId}&api_key=${config.API_KEY}`;
                const res = await axios.get(apiUrl, { timeout: 20000 });

                if (res.data.status !== 'success') {
                    throw new Error(res.data.message || 'API Error');
                }
                const downloadLink = res.data.data.download_url;
                const songTitle = res.data.data.title || data.title;
                const thumbnail = res.data.data.thumbnail || data.thumbnail;
                await socket.sendMessage(sender, { react: { text: '⬆️', key: mek.key } });
                const fileName = songTitle.replace(/[^a-zA-Z0-9]/g, '_');
                if (text === '1') {
                    await socket.sendMessage(sender, {
                        audio: { url: downloadLink },
                        mimetype: 'audio/mpeg'
                    }, { quoted: mek });
                } else if (text === '2') {
                    await socket.sendMessage(sender, {
                        document: { url: downloadLink },
                        mimetype: 'audio/mpeg',
                        fileName: `${fileName}.mp3`,
                        caption: songTitle
                    }, { quoted: mek });
                }

                await socket.sendMessage(sender, { react: { text: '✅', key: mek.key } });

            } catch (err) {
                await socket.sendMessage(sender, {
                    text: '❌ DOWNLOAD ERROR\n\n' + err.message
                }, { quoted: mek });

                await socket.sendMessage(sender, { react: { text: '❌', key: mek.key } });
            }
        };

        socket.ev.on('messages.upsert', listener);
        setTimeout(() => {
            socket.ev.off('messages.upsert', listener);
        }, 300000);

    } catch (err) {
        await socket.sendMessage(sender, {
            text: '❌ ERROR\n\n' + err.message
        }, { quoted: msg });
    }

    break;  
                 case 'tiktok':
    if (!args.length || !args.join(' ').startsWith('https://')) {
        await socket.sendMessage(sender, {
            image: { url: config.ERROR },
            caption: `❌ ERROR

Please provide a valid TikTok URL!

📋 Example: .tiktok  https://www.tiktok.com/@user/video/xyz`
        });
        break;
    }

    await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

    let tiktokTimeout;

    try {
        const tiktokUrl = args.join(' ');
        const response = await axios.get(`${config.API_MAIN_URL}/tiktok/download?url=${encodeURIComponent(tiktokUrl)}&api_key=${config.API_KEY}`);
        const tiktokData = response.data.result;

        if (!response.data.status || !tiktokData) {
            await socket.sendMessage(sender, {
                image: { url: config.ERROR },
                caption: `❌ ERROR

Failed to fetch TikTok video! Please try again later.`
            });
            break;
        }

        const captionMessage = `☘️ *TIKTOK DOWNLOADER*

📝 Title: ${tiktokData.title || 'TikTok Video'}
👤 Author: ${tiktokData.author?.nickname || 'Unknown'}
❤️ Likes: ${tiktokData.digg_count?.toLocaleString() || 'N/A'}
👀 Views: ${tiktokData.play_count?.toLocaleString() || 'N/A'}
💬 Comments: ${tiktokData.comment_count?.toLocaleString() || 'N/A'}
⏱️ Duration: ${tiktokData.duration || 'N/A'} seconds

⬇️ DOWNLOAD OPTIONS

🔢 Reply with a number:

*1 ║❯❯ No Watermark*
*2 ║❯❯ With Watermark*
*3 ║❯❯ Audio Only*`;

        const sentMessage = await socket.sendMessage(sender, {
            image: { url: tiktokData.cover || config.SITHIJA_IMAGE_PATH },
            caption: captionMessage
        }, { quoted: msg });

        const messageID = sentMessage.key.id;

        const handleTikTokSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const userResponse = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                if (tiktokTimeout) clearTimeout(tiktokTimeout);
                
                await socket.sendMessage(sender, { react: { text: '⬇️', key: replyMek.key } });

                const downloadLinks = tiktokData.downloads;
                let mediaMessage;

                try {
                    switch (userResponse) {
                        case '1':
                            mediaMessage = {
                                video: { url: downloadLinks.no_watermark },
                                mimetype: 'video/mp4',
                                caption: `✅ TIKTOK VIDEO

No Watermark Video
📝 ${tiktokData.title}`
                            };
                            break;
                        case '2':
                            mediaMessage = {
                                video: { url: downloadLinks.watermark },
                                mimetype: 'video/mp4',
                                caption: `✅ TIKTOK VIDEO

With Watermark Video
📝 ${tiktokData.title}`
                            };
                            break;
                        case '3':
                            mediaMessage = {
                                audio: { url: downloadLinks.audio },
                                mimetype: 'audio/mpeg',
                                caption: `✅ TIKTOK AUDIO

Audio Only
📝 ${tiktokData.title}`
                            };
                            break;

                        default:
                            await socket.sendMessage(sender, {
                                image: { url: config.ERROR },
                                caption: `❌ INVALID SELECTION

Please reply with 1, 2, 3, or 4.`
                            });
                            return;
                    }

                    await socket.sendMessage(sender, mediaMessage, { quoted: replyMek });
                    await socket.sendMessage(sender, { react: { text: '✅', key: replyMek.key } });

                } catch (sendError) {
                    console.error('TikTok send error:', sendError);
                    await socket.sendMessage(sender, {
                        image: { url: config.ERROR },
                        caption: `❌ ERROR

Failed to send: ${sendError.message}`
                    }, { quoted: replyMek });
                } finally {
                    socket.ev.off('messages.upsert', handleTikTokSelection);
                }
            }
        };

        socket.ev.on('messages.upsert', handleTikTokSelection);

        tiktokTimeout = setTimeout(() => {
            socket.ev.off('messages.upsert', handleTikTokSelection);
            console.log('TikTok selection timeout - cleaned up');
        }, 120000);

    } catch (error) {
        console.error('TikTok download error:', error);
        await socket.sendMessage(sender, {
            image: { url: config.ERROR },
            caption: `❌ ERROR

Failed to process TikTok request: ${error.message}`
        });
    }
    break;
case 'film':
case 'tvs': {
    const isTvMode = command === 'tvs';   // .tvs නම් TV, .film නම් Movie
    
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                `*කරුණාකර ${isTvMode ? 'TV series' : 'චිත්‍රපටය'}ේ නම ලබාදෙන්න!*\nඋදා: .${command} avatar`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const query = args.join(' ');
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';
    const BASE = 'https://api.chamindu.site/api/v1/movies';

    await socket.sendMessage(sender, { 
        text: `📽️ 𝙎𝙚𝙖𝙧𝙘𝙝𝙞𝙣𝙜 𝙤𝙣 𝙎𝙞𝙣𝙝𝙖𝙡𝙖𝙎𝙪𝙗...` 
    });

    try {
        // ───── 1. SEARCH ─────
        const searchRes = await axios.get(
            `${BASE}/sinhalasub/search?q=${encodeURIComponent(query)}&api_key=${API_KEY}`
        );
        const searchData = searchRes.data;

        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    `*SinhalaSub හි "${query}" හමුවෙන්නේ නැත! 😞*`,
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        // Filter by mode: .tvs → tvshows, .film → movies
        const wantedType = isTvMode ? 'tvshows' : 'movies';
        let results = searchData.data.filter(r => r.type === wantedType);

        // fallback — type නැත්නම් ඔක්කොම පෙන්නනවා
        if (results.length === 0) results = searchData.data;

        results = results.slice(0, 25);

        let listText = `☘️ *𝗦𝗜𝗡𝗛𝗔𝗟𝗔𝗦𝗨𝗕 : _${isTvMode ? '𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦' : '𝗠𝗢𝗩𝗜𝗘'} 𝗥𝗘𝗦𝗨𝗟𝗧𝗦_* 🔍
╭──────●➤
🔎 *𝗤𝘂𝗲𝗿𝘆 ➟* _${query}_
📊 *Status ➟* _${results.length} Results Found_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤
💡 *𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 𝘁ᴏ 𝗦𝗲𝗹𝗲𝗰𝘁*
*╭──────●➤*\n\n`;

        results.forEach((item, index) => {
            listText += `*♦️ ${index + 1} ║❯❯ ${item.type === 'tvshows' ? '📺' : '🎬'} | ${item.title}*\n`;
        });

        listText += `╰──────────●➤\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE },
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;

        // ───── 2. LIST SELECTION ─────
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const text = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (!isReply || sender !== replyMek.key.remoteJid) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= results.length) {
                await socket.sendMessage(sender, {
                    image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                    caption: formatMessage(
                        '❌ INVALID SELECTION',
                        `*වැරදි අංකයක්! 1-${results.length} අතර තෝරන්න! 😕*`,
                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                    )
                }, { quoted: replyMek });
                return;
            }

            const selected = results[choice];
            const isTv = selected.type === 'tvshows' || selected.link.includes('/tvshows/') || selected.link.includes('/episodes/');

            // ─────────── MOVIE FLOW ───────────
            if (!isTv) {
                await socket.sendMessage(sender, { 
                    text: '📽️ 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙢𝙤𝙫𝙞𝙚 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
                }, { quoted: replyMek });

                try {
                    const infoRes = await axios.get(
                        `${BASE}/sinhalasub/infodl?q=${encodeURIComponent(selected.link)}&api_key=${API_KEY}`
                    );
                    const infoData = infoRes.data;

                    if (!infoData.status || !infoData.data) throw new Error('Failed to fetch details');

                    const movie = infoData.data;
                    const downloads = (movie.downloads || []).filter(d => d && d.link);

                    if (downloads.length === 0) {
                        await socket.sendMessage(sender, {
                            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                            caption: formatMessage('❌ NO DOWNLOADS', '*බාගත කිරීමේ link නොමැත!*', `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
                        }, { quoted: replyMek });
                        return;
                    }

                    // Cast names only
                    const castNames = Array.isArray(movie.cast)
                        ? movie.cast.slice(0, 5).map(c => c.name).join(', ')
                        : 'N/A';

                    const story = movie.story
                        ? movie.story.substring(0, 200) + '...'
                        : 'N/A';

                    const detailsCaption = formatMessage(
                        `☘️ *𝗧ɪᴛʟᴇ ➟* _${movie.title}_`,
                        `▫️🥇 *𝗜𝗠𝗗𝗕 ➟* _${movie.imdb || 'N/A'}_
▫️🌎 *𝗟𝗮𝗻𝗴𝘂𝗮𝗴𝗲 ➟* _${movie.language || 'N/A'}_
▫️🎭 *𝗚𝗲𝗻𝗿𝗲𝘀 ➟* _${(movie.genres || []).join(', ') || 'N/A'}_
▫️🎬 *𝗗𝗶𝗿𝗲𝗰𝘁𝗼𝗿 ➟* _${movie.director || 'N/A'}_
▫️👥 *𝗖𝗮𝘀𝘁 ➟* _${castNames}_
*➟➟➟➟➟➟➟➟➟➟*
*📖 𝗦𝗧𝗢𝗥𝗬 ➟* _${story}_`,
                        `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                    );

                    await socket.sendMessage(sender, {
                        image: { url: movie.image || sessionConfig.LAKIYA_IMAGE_PATH || config.LAKIYA_IMAGE_PATH },
                        caption: detailsCaption
                    }, { quoted: replyMek });

                    // Download options list
                    let dlText = formatMessage(
                        `⬇️🍀 *𝗦𝗜𝗡𝗛𝗔𝗟𝗔𝗦𝗨𝗕 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*`,
                        `${downloads.map((d, i) => `▫️ *${(i + 1).toString().padStart(2, '0')} ❱❱ 📥 ${d.quality} (${d.size || 'N/A'})*`).join('\n')}

╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤`,
                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                    );

                    const dlMsg = await socket.sendMessage(sender, { text: dlText }, { quoted: replyMek });
                    const dlMsgID = dlMsg.key.id;

                    // ───── Movie quality selection ─────
                    const handleMovieQuality = async ({ messages: qMsgs }) => {
                        const qMek = qMsgs[0];
                        if (!qMek?.message) return;

                        const qText = qMek.message.conversation || qMek.message.extendedTextMessage?.text;
                        const isQReply = qMek.message.extendedTextMessage?.contextInfo?.stanzaId === dlMsgID;

                        if (!isQReply || sender !== qMek.key.remoteJid) return;

                        const qIdx = parseInt(qText) - 1;
                        if (isNaN(qIdx) || qIdx < 0 || qIdx >= downloads.length) {
                            await socket.sendMessage(sender, {
                                image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                caption: formatMessage('❌ INVALID', `*1-${downloads.length} අතර තෝරන්න!*`, `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
                            }, { quoted: qMek });
                            return;
                        }

                        const chosen = downloads[qIdx];

                        await socket.sendMessage(sender, { react: { text: '📥', key: qMek.key } });

                        // Skip telegram links (Bot can't download them directly)
                        const isTelegram = chosen.link.includes('telegram.me') || chosen.link.includes('t.me');

                        await socket.sendMessage(sender, {
                            document: { url: chosen.link },
                            mimetype: 'video/mp4',
                            fileName: `${movie.title} - ${chosen.quality}.mp4`,
                            caption: formatMessage(
                                `☘️ ${movie.title}`,
                                `\`❚█═${sessionConfig.MOVIE_CAPTION || config.MOVIE_CAPTION}═█❚\`
                                
\`[${chosen.quality} - ${chosen.size}]\``,
                                `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                            )
                        }, { quoted: qMek });

                        await socket.sendMessage(sender, { react: { text: '✅', key: qMek.key } });

                        // Cleanup
                        socket.ev.off('messages.upsert', handleMovieQuality);
                        socket.ev.off('messages.upsert', handleSelection);
                    };

                    socket.ev.on('messages.upsert', handleMovieQuality);

                } catch (err) {
                    console.error('Movie info error:', err);
                    await socket.sendMessage(sender, {
                        image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                        caption: formatMessage('❌ ERROR', `*${err.message}*`, `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
                    }, { quoted: replyMek });
                    socket.ev.off('messages.upsert', handleSelection);
                }
            }
            // ─────────── TV FLOW ───────────
            else {
                await socket.sendMessage(sender, { 
                    text: '📺 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙏𝙑 𝙚𝙥𝙞𝙨𝙤𝙙𝙚 𝙡𝙞𝙣𝙠𝙨...' 
                }, { quoted: replyMek });

                try {
                    const tvRes = await axios.get(
                        `${BASE}/sinhalasub/tv/dl?q=${encodeURIComponent(selected.link)}&api_key=${API_KEY}`
                    );
                    const tvData = tvRes.data;

                    if (!tvData.status || !tvData.data || tvData.data.length === 0) {
                        throw new Error('No TV episode links found');
                    }

                    const tvDownloads = tvData.data.filter(d => d && d.link);

                    let tvText = formatMessage(
                        `☘️ *𝗧𝗩-𝗦𝗘𝗥𝗜𝗘𝗦 : _𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦_* 📺`,
                        `🎬 *${selected.title}*

${tvDownloads.map((d, i) => `▫️ *${(i + 1).toString().padStart(2, '0')} ❱❱ 📥 ${d.quality} (${d.size || 'N/A'})*`).join('\n')}

╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤`,
                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                    );

                    const tvMsg = await socket.sendMessage(sender, { text: tvText }, { quoted: replyMek });
                    const tvMsgID = tvMsg.key.id;

                    const handleTvQuality = async ({ messages: tvMsgs }) => {
                        const tvMek = tvMsgs[0];
                        if (!tvMek?.message) return;

                        const tvTxt = tvMek.message.conversation || tvMek.message.extendedTextMessage?.text;
                        const isTvReply = tvMek.message.extendedTextMessage?.contextInfo?.stanzaId === tvMsgID;

                        if (!isTvReply || sender !== tvMek.key.remoteJid) return;

                        const tvIdx = parseInt(tvTxt) - 1;
                        if (isNaN(tvIdx) || tvIdx < 0 || tvIdx >= tvDownloads.length) {
                            await socket.sendMessage(sender, {
                                image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                                caption: formatMessage('❌ INVALID', `*1-${tvDownloads.length} අතර තෝරන්න!*`, `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
                            }, { quoted: tvMek });
                            return;
                        }

                        const chosenTv = tvDownloads[tvIdx];

                        await socket.sendMessage(sender, { react: { text: '📥', key: tvMek.key } });

                        await socket.sendMessage(sender, {
                            document: { url: chosenTv.link },
                            mimetype: 'video/mp4',
                            fileName: `${selected.title.replace(/ Sinhala Subtitles$/, '')} - ${chosenTv.quality}.mp4`,
                            caption: formatMessage(
                                `☘️ ${selected.title}`,
                                `\`[Episode File - ${chosenTv.quality} - ${chosenTv.size}]\``,
                                `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                            )
                        }, { quoted: tvMek });

                        await socket.sendMessage(sender, { react: { text: '✅', key: tvMek.key } });

                        socket.ev.off('messages.upsert', handleTvQuality);
                        socket.ev.off('messages.upsert', handleSelection);
                    };

                    socket.ev.on('messages.upsert', handleTvQuality);

                } catch (err) {
                    console.error('TV error:', err);
                    await socket.sendMessage(sender, {
                        image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                        caption: formatMessage('❌ ERROR', `*${err.message}*`, `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
                    }, { quoted: replyMek });
                    socket.ev.off('messages.upsert', handleSelection);
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('SinhalaSub command error:', error);
        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }

    break;
      
                 
    break;    case 'menu':
               case 'alive':     {
    try {
        const pushName = msg.pushName || 'User';
        const date = new Date();
        const slstDate = new Date(date.toLocaleString("en-US", { timeZone: "Asia/Colombo" }));
        const formattedDate = `${slstDate.getFullYear()}/${slstDate.getMonth() + 1}/${slstDate.getDate()}`;
        const formattedTime = slstDate.toLocaleTimeString();
        
        const hour = slstDate.getHours();
      
        const greetings = hour < 12 ? `Good Morning✨` :
                          hour < 15 ? `Good Afternoon🚀` :
                          hour < 18 ? `Good Evening! 🌟` : `Good Night🌙`;
        const prefix = sessionConfig.PREFIX || config.PREFIX || '.';

        // Main Menu (Number reply removed)
        const mainMenuMsg = `*🌟 𝙃𝙚𝙮 ❟ ${pushName} ✨𝙃𝙤𝙬 𝙖𝙧𝙚 𝙮𝙤𝙪.*      
*╭─「 ᴄᴏᴍᴍᴀɴᴅꜱ ᴘᴀɴᴇʟ」*
*┃ \`🐸 ${greetings}\`*
*┃ \`🧩 𝚃𝚒𝚖𝚎\` : ${formattedTime}*
*┃ \`🦊 𝙳𝚊𝚝𝚎\` : ${formattedDate}*
*┃ \`🤡 𝙱𝚘𝚝 𝙽𝚊𝚖𝚎:\` ɢʜᴏsᴛ*
*┃ \`🐞 𝙿𝚕𝚊𝚝𝚏𝚘𝚛𝚖:\` Linux*
*╰────────●●►*    
*╭─「 ᴄᴏᴍᴍᴀɴᴅꜱ ᴘᴀɴᴇʟ」*
│ 🎡 .film
│ 🎡 .thenkiri
│ 🎡 .ping
│ 🎡 .song
│ 🎡 .tiktok
│ 🎡 .menu
│ 🎡 .alive
*╰────────●●►*   
> ${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`;

        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE},
            caption: mainMenuMsg
        }, { quoted: msg });

       

    } catch (e) {
        console.error(e);
    }
}
break;    
case 'thenkiri': {
    if (!args.length) {
        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                '*කරුණාකර චිත්‍රපටයේ හෝ TV series එකේ නම ලබාදෙන්න!*\nඋදා: .thenkiri avatar',
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
        break;
    }

    const query = args.join(' ');
    const API_KEY = 'chama_api_11230a80e5eed3c1b80bfcc5d1773ec9';
    const BASE = 'https://api.chamindu.site/api/v1/movies';

    await socket.sendMessage(sender, { 
        text: '📽️ 𝙎𝙚𝙖𝙧𝙘𝙝𝙞𝙣𝙜 𝙤𝙣 𝙏𝙝𝙚𝙣𝙠𝙞𝙧𝙞...' 
    });

    try {
        // ───── 1. SEARCH ─────
        const searchRes = await axios.get(
            `${BASE}/thenkiri/search?q=${encodeURIComponent(query)}&api_key=${API_KEY}`
        );
        const searchData = searchRes.data;

        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                caption: formatMessage(
                    '❌ NO RESULTS',
                    `*Thenkiri හි "${query}" හමුවෙන්නේ නැත! 😞*`,
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                )
            }, { quoted: msg });
            break;
        }

        const results = searchData.data.slice(0, 25);

        let listText = `☘️ *𝗧𝗛𝗘𝗡𝗞𝗜𝗥𝗜 : _𝗦𝗘𝗔𝗥𝗖𝗛 𝗥𝗘𝗦𝗨𝗟𝗧𝗦_* 🔍
╭──────●➤
🔎 *𝗤𝘂𝗲𝗿𝘆 ➟* _${query}_
📊 *Status ➟* _${results.length} Results Found_
╰──────────●➤
╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤
💡 *𝗥ᴇᴘʟʏ ᴡɪᴛʜ ᴀ 𝗡ᴜᴍʙᴇʀ 𝘁ᴏ 𝗦𝗲𝗹𝗲𝗰𝘁*
*╭──────●➤*\n\n`;

        results.forEach((item, index) => {
            const icon = item.type === 'tvshows' ? '📺 TV' : '🎬 Movie';
            listText += `*♦️ ${index + 1} ║❯❯ ${icon} | ${item.title}*\n`;
        });

        listText += `╰──────────●➤\n> ${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`;

        const sentMsg = await socket.sendMessage(sender, {
            image: { url: config.BOT_IMAGE },
            caption: listText
        }, { quoted: msg });

        const messageID = sentMsg.key.id;

        // ───── 2. LIST SELECTION ─────
        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const text = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReply = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (!isReply || sender !== replyMek.key.remoteJid) return;

            const choice = parseInt(text) - 1;
            if (isNaN(choice) || choice < 0 || choice >= results.length) {
                await socket.sendMessage(sender, {
                    image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                    caption: formatMessage(
                        '❌ INVALID SELECTION',
                        `*වැරදි අංකයක්! 1-${results.length} අතර තෝරන්න! 😕*`,
                        `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                    )
                }, { quoted: replyMek });
                return;
            }

            const selected = results[choice];
            const isTv = selected.type === 'tvshows' || /tv-series|complete/i.test(selected.title);

            await socket.sendMessage(sender, { 
                text: isTv ? '📺 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙏𝙑 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' : '📽️ 𝙁𝙚𝙩𝙘𝙝𝙞𝙣𝙜 𝙢𝙤𝙫𝙞𝙚 𝙙𝙚𝙩𝙖𝙞𝙡𝙨...' 
            }, { quoted: replyMek });

            try {
                // ───── 3. INFODL (both movies & TV) ─────
                const infoRes = await axios.get(
                    `${BASE}/thenkiri/infodl?q=${encodeURIComponent(selected.link)}&api_key=${API_KEY}`
                );
                const infoData = infoRes.data;

                if (!infoData.status || !infoData.data) throw new Error('Failed to fetch details');

                const info = infoData.data;
                const downloads = (info.downloads || []).filter(d => d && d.link);

                if (downloads.length === 0) {
                    await socket.sendMessage(sender, {
                        image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                        caption: formatMessage('❌ NO DOWNLOADS', '*බාගත කිරීමේ link නොමැත!*', `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
                    }, { quoted: replyMek });
                    socket.ev.off('messages.upsert', handleSelection);
                    return;
                }

                // ───── Details Caption ─────
                const castNames = Array.isArray(info.cast) && info.cast.length
                    ? info.cast.slice(0, 5).map(c => c.name).join(', ')
                    : 'N/A';

                const story = info.story
                    ? info.story.substring(0, 200) + (info.story.length > 200 ? '...' : '')
                    : 'N/A';

                const detailsCaption = formatMessage(
                    `☘️ *𝗧ɪᴛʟᴇ ➟* _${info.title || selected.title}_`,
                    `▫️🥇 *𝗜𝗠𝗗𝗕 ➟* _${info.imdb || 'N/A'}_
▫️🌎 *𝗟𝗮𝗻𝗴𝘂𝗮𝗴𝗲 ➟* _${info.language || 'N/A'}_
▫️🎭 *𝗚𝗲𝗻𝗿𝗲𝘀 ➟* _${(info.genres || []).join(', ') || 'N/A'}_
▫️🎬 *𝗗𝗶𝗿𝗲𝗰𝘁𝗼𝗿 ➟* _${info.director || 'N/A'}_
▫️👥 *𝗖𝗮𝘀𝘁 ➟* _${castNames}_
*➟➟➟➟➟➟➟➟➟➟*
*📖 𝗦𝗧𝗢𝗥𝗬 ➟* _${story}_`,
                    `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                );

                await socket.sendMessage(sender, {
                    image: { url: info.image || sessionConfig.LAKIYA_IMAGE_PATH || config.LAKIYA_IMAGE_PATH },
                    caption: detailsCaption
                }, { quoted: replyMek });

                // ───── Download Options (dynamic: movies = qualities, TV = episodes) ─────
                const headerTitle = isTv
                    ? `⬇️🍀 *𝗧𝗛𝗘𝗡𝗞𝗜𝗥𝗜 𝗧𝗩 𝗘𝗣𝗜𝗦𝗢𝗗𝗘𝗦*`
                    : `⬇️🍀 *𝗧𝗛𝗘𝗡𝗞𝗜𝗥𝗜 𝗗𝗢𝗪𝗡𝗟𝗢𝗔𝗗 𝗢𝗣𝗧𝗜𝗢𝗡𝗦*`;

                // Parse episode info from name/size
                const parsedDownloads = downloads.map(d => {
                    const epMatch = d.name.match(/S\d+E\d+/i);
                    const sizeMatch = d.name.match(/\(([\d.]+\s*[MGK]B)\)/i);
                    const qualityMatch = d.name.match(/(FHD 1080p|HD 720p|SD 480p|1080p|720p|480p)/i);

                    return {
                        ...d,
                        episode: epMatch ? epMatch[0] : null,
                        size: sizeMatch ? sizeMatch[1] : (d.size || 'N/A'),
                        quality: qualityMatch ? qualityMatch[1] : (d.quality || 'N/A')
                    };
                });

                let dlText = formatMessage(
                    headerTitle,
                    `${parsedDownloads.map((d, i) => {
                        const label = isTv && d.episode
                            ? `${d.episode} (${d.size})`
                            : `${d.quality} (${d.size})`;
                        return `▫️ *${(i + 1).toString().padStart(2, '0')} ❱❱ 📥 ${label}*`;
                    }).join('\n')}

╭──────●➤
*🔢 ʀᴇᴘʟʏ ʙᴇʟᴏᴡ ɴᴜᴍʙᴇʀ*
╰──────────●➤`,
                    `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                );

                const dlMsg = await socket.sendMessage(sender, { text: dlText }, { quoted: replyMek });
                const dlMsgID = dlMsg.key.id;

                // ───── 4. QUALITY/EPISODE SELECTION ─────
                const handleQuality = async ({ messages: qMsgs }) => {
                    const qMek = qMsgs[0];
                    if (!qMek?.message) return;

                    const qText = qMek.message.conversation || qMek.message.extendedTextMessage?.text;
                    const isQReply = qMek.message.extendedTextMessage?.contextInfo?.stanzaId === dlMsgID;

                    if (!isQReply || sender !== qMek.key.remoteJid) return;

                    const qIdx = parseInt(qText) - 1;
                    if (isNaN(qIdx) || qIdx < 0 || qIdx >= parsedDownloads.length) {
                        await socket.sendMessage(sender, {
                            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                            caption: formatMessage('❌ INVALID', `*1-${parsedDownloads.length} අතර තෝරන්න!*`, `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
                        }, { quoted: qMek });
                        return;
                    }

                    const chosen = parsedDownloads[qIdx];

                    await socket.sendMessage(sender, { react: { text: '📥', key: qMek.key } });

                    // Build filename
                    const cleanTitle = (info.title || selected.title).replace(/[^\w\s.-]/g, '').trim();
                    const fileName = isTv && chosen.episode
                        ? `${cleanTitle} - ${chosen.episode}.mkv`
                        : `${cleanTitle} - ${chosen.quality}.mp4`;

                    const captionLabel = isTv && chosen.episode
                        ? `[${chosen.episode} - ${chosen.size}]`
                        : `[${chosen.quality} - ${chosen.size}]`;

                    await socket.sendMessage(sender, {
                        document: { url: chosen.link },
                        mimetype: 'video/mp4',
                        fileName: fileName,
                        caption: formatMessage(
                            `☘️ ${info.title || selected.title}`,
                            `\`❚█═${sessionConfig.MOVIE_CAPTION || config.MOVIE_CAPTION}═█❚\`

\`${captionLabel}\``,
                            `${sessionConfig.MOVIE_FOOTER || config.MOVIE_FOOTER}`
                        )
                    }, { quoted: qMek });

                    await socket.sendMessage(sender, { react: { text: '✅', key: qMek.key } });

                    // ───── Cleanup listeners ─────
                    socket.ev.off('messages.upsert', handleQuality);
                    socket.ev.off('messages.upsert', handleSelection);
                };

                socket.ev.on('messages.upsert', handleQuality);

            } catch (err) {
                console.error('Thenkiri info error:', err);
                await socket.sendMessage(sender, {
                    image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                    caption: formatMessage('❌ ERROR', `*${err.message}*`, `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`)
                }, { quoted: replyMek });
                socket.ev.off('messages.upsert', handleSelection);
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('Thenkiri command error:', error);
        await socket.sendMessage(sender, {
            image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
            caption: formatMessage(
                '❌ ERROR',
                `*දෝෂයක් ඇතිවුණා:* ${error.message || 'Unknown error'}`,
                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
            )
        }, { quoted: msg });
    }

    break;
}
                case 'set':
                case 'setting': {
                    if (!isOwner) {
                        return await socket.sendMessage(sender, {
                            text: "❌ *Only the bot owner can use this command.*"
                        }, { quoted: msg });
                    }

                    if (!args.length) {
                        let helpText = `🎀 *𝗦𝗬𝗦𝗧𝗘𝗠  𝗖𝗢𝗡𝗙𝗜𝗚𝗨𝗥𝗔𝗧𝗜𝗢𝗡  𝗣𝗔𝗡𝗘𝗟*\n\n` +
                            `📝 *𝖴𝗌𝖺𝗀𝖾 :* \`.set KEY:VALUE\`\n` +
                            `✨ *𝖤𝗑𝖺𝗆𝗉𝗅𝖾 :* \`.set MODE:public\`\n` +
                            `🫧 *𝖬𝗎𝗅𝗍𝗂 :* \`.set PREFIX:!\`\n\n` +
                            `🐞 *𝖠𝗏𝖺𝗂𝗅𝖺𝖻𝗅𝖾  \𝖲𝗒𝗌𝗍𝖾𝗆  𝖪𝖾𝗒𝗌 :*\n` +
                            `🐞 \`AUTO_RECORDING\`\n` +
                            `🐞 \`AUTO_TYPING\`\n` +
                            `🐞 \`PREFIX\`\n` +
                            `🐞 \`MODE\` (public/private)\n` +
                            `🐞 \`BOT_IMAGE\`\n` +
                            `🐞 \`AIR_FOOTER\`\n` +
                            `🐞 \`BOT_NAME\`\n`;

                        return await socket.sendMessage(sender, {
                            image: { url: config.BOT_IMAGE || config.ERROR },
                            caption: formatMessage(
                                `𝗖𝗢𝗡𝗙𝗜𝗚  𝗠𝗔𝗡𝗔𝗚𝗘𝗥  ⚙️`,
                                helpText,
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: msg });
                    }

                    const input = args.join(' ');
                    const updates = {};
                    const validKeys = [
                        'PREFIX', 'AUTO_RECORDING', 'AUTO_TYPING',
                        'BOT_NAME', 'AIR_FOOTER', 'JID', 'MODE'
                    ];

                    const pairs = input.split(',');
                    let hasInvalidKey = false;
                    let invalidKeyName = '';

                    pairs.forEach(pair => {
                        let [key, ...valueParts] = pair.split(':');
                        if (!key || valueParts.length === 0) return;

                        key = key.trim().toUpperCase();
                        let value = valueParts.join(':').trim();

                        if (validKeys.includes(key)) {
                            if (value.toLowerCase() === 'true') {
                                updates[key] = 'true';
                            } else if (value.toLowerCase() === 'false') {
                                updates[key] = 'false';
                            } else {
                                updates[key] = value;
                            }
                        } else {
                            hasInvalidKey = true;
                            invalidKeyName = key;
                        }
                    });

                    if (hasInvalidKey) {
                        return await socket.sendMessage(sender, {
                            text: `Invalid system key: \`${invalidKeyName}\`\n\n> ${sessionConfig.AIR_FOOTER || config.AIR_FOOTER}`
                        }, { quoted: msg });
                    }

                    if (Object.keys(updates).length === 0) {
                        return await socket.sendMessage(sender, { text: "🎀 *𝗙𝗢𝗥𝗠𝗔𝗧  𝗘𝗥𝗥𝗢𝗥:* Please use `Key:Value` structure." });
                    }

                    try {
                        await socket.sendMessage(sender, { react: { text: "⚙️", key: msg.key } });

                        sessionConfig = { ...sessionConfig, ...updates };
                        await updateUserConfig(sanitizedNumber, sessionConfig);
                        activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

                        let updateSummary = Object.entries(updates).map(([k, v]) => {
                            let displayVal = Array.isArray(v) ? v.join(' ') : v;
                            return `🎀 *${k}* ──❯ \`${displayVal}\``;
                        }).join('\n');

                        const successMsg = `🎀 *𝗖𝗢𝗡𝗙𝗜𝗚𝗨𝗥𝗔𝗧𝗜𝗢𝗡  𝗨𝗣𝗗𝗔𝗧𝗘𝗗*\n\n` +
                            `${updateSummary}\n\n` +
                            `🫧 _System cloud changes applied successfully._`;

                        await socket.sendMessage(sender, {
                            image: { url: config.BOT_IMAGE },
                            caption: formatMessage(
                                `✅ 𝗨𝗣𝗗𝗔𝗧𝗘  𝗦𝗨𝗖𝗖𝗘𝗦𝗦  ✅`,
                                successMsg,
                                `${sessionConfig.BOT_FOOTER || config.BOT_FOOTER}`
                            )
                        }, { quoted: msg });

                        await socket.sendMessage(sender, { react: { text: "✨", key: msg.key } });

                    } catch (error) {
                        console.error("Update Error:", error);
                        await socket.sendMessage(sender, { text: "🎀 " + error.message });
                    }
                }
                break;

                
            }
        } catch (error) {
            console.error('Command handler error:', error);
            await socket.sendMessage(sender, {
                text: `❌ ERROR\nAn error occurred: ${error.message}`,
            });
        }
    });
}
async function setupMessageHandlers(socket) {
    const messageHandler = async ({ messages }) => {
        const msg = messages[0];
        if (!msg.message || msg.key.remoteJid === 'status@broadcast') return;

        const senderNumber = msg.key.participant ? msg.key.participant.split('@')[0] : msg.key.remoteJid.split('@')[0];
        const botNumber = jidNormalizedUser(socket.user.id).split('@')[0];
        const isReact = msg.message.reactionMessage;

        const sanitizedNumber = botNumber.replace(/[^0-9]/g, '');
        const sessionConfig = activeSockets.get(sanitizedNumber)?.config || config;

        if (sessionConfig.AUTO_TYPING === 'true') {
            try {
                await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
            } catch (error) {
                
            }
        }

        if (sessionConfig.AUTO_RECORDING === 'true') {
            try {
                await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
            } catch (error) {
               
            }
        }

        if (!isReact && senderNumber !== botNumber) {
            if (sessionConfig.AUTO_REACT === 'true') {
                const reactions = [
                    '❤', '💕', '😻', '🧡', '💛', '💚', '💙', '💜', '🖤', '❣', '💞', '💓', '💗',
                    '💖', '💘', '💝', '💟', '♥', '💌', '🙂', '🤗', '😌', '😉', '🤗', '😊',
                    '🎊', '🎉', '🎁', '🎈', '👋'
                ];
                const randomReaction = reactions[Math.floor(Math.random() * reactions.length)];

                await new Promise(resolve => setTimeout(resolve, Math.floor(Math.random() * 2000) + 1000));

                try {
                    await socket.sendMessage(msg.key.remoteJid, { react: { text: randomReaction, key: msg.key } });
                } catch (error) {
                    
                }
            }
        }
    };

    socket.ev.on('messages.upsert', messageHandler);
    return () => {
        socket.ev.off('messages.upsert', messageHandler);
       
    };
}

async function saveSession(number, creds) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            { creds, updatedAt: new Date() },
            { upsert: true }
        );
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        fs.ensureDirSync(sessionPath);
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(creds, null, 2));
        let numbers = [];
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
        }
        if (!numbers.includes(sanitizedNumber)) {
            numbers.push(sanitizedNumber);
            fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
        }
    } catch (error) {
      
    }
}

async function restoreSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const session = await Session.findOne({ number: sanitizedNumber });
        if (!session || !session.creds || !session.creds.me || !session.creds.me.id) {
            await deleteSession(sanitizedNumber);
            return null;
        }
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        fs.ensureDirSync(sessionPath);
        fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(session.creds, null, 2));
        return session.creds;
    } catch (error) {
        return null;
    }
}

async function deleteSession(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.deleteOne({ number: sanitizedNumber });
        const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
        if (fs.existsSync(sessionPath)) {
            fs.removeSync(sessionPath);
        }
        if (fs.existsSync(NUMBER_LIST_PATH)) {
            let numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
            numbers = numbers.filter(n => n !== sanitizedNumber);
            fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
        }
    } catch (error) {
        
    }
}

async function loadUserConfig(number) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        const configDoc = await Session.findOne({ number: sanitizedNumber }, 'config');
        return { ...config, ...configDoc?.config };
    } catch (error) {
        console.error(`Failed to load config for ${number}:`, error);
        return { ...config };
    }
}

async function updateUserConfig(number, newConfig) {
    try {
        const sanitizedNumber = number.replace(/[^0-9]/g, '');
        await Session.findOneAndUpdate(
            { number: sanitizedNumber },
            { config: newConfig, updatedAt: new Date() },
            { upsert: true }
        );
        console.log(`Updated config for ${sanitizedNumber}`);
    } catch (error) {
        console.error(`Failed to update config for ${sanitizedNumber}:`, error);
        throw error;
    }
} 
function setupAutoRestart(socket, number) {
    const maxReconnectAttempts = 10;
    let reconnectAttempts = 0;

    socket.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'close' && lastDisconnect?.error?.output?.statusCode !== 401) {
            if (reconnectAttempts >= maxReconnectAttempts) {
                activeSockets.delete(number.replace(/[^0-9]/g, ''));
                socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
                return;
            }
            console.log(`Connection lost for ${number}, attempt ${reconnectAttempts + 1}/${maxReconnectAttempts}`);
            try {
                await delay(5000 * (reconnectAttempts + 1));
                activeSockets.delete(number.replace(/[^0-9]/g, ''));
                socketCreationTime.delete(number.replace(/[^0-9]/g, ''));
                const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
                await EmpirePair(number, mockRes);
                reconnectAttempts = 0;
            } catch (error) {
                console.error(`Reconnect failed for ${number}:`, error);
                reconnectAttempts++;
            }
        } else if (connection === 'open') {
            reconnectAttempts = 0;
            console.log(`Connection established for ${number}`);
        }
    });
}
async function EmpirePair(number, res) {
    const sanitizedNumber = number.replace(/[^0-9]/g, '');
    const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);

    await restoreSession(sanitizedNumber);
    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);

    try {
        const { version } = await fetchLatestBaileysVersion();
        const socket = makeWASocket({
            auth: state,
            printQRInTerminal: false,
            version,
            browser: Browsers.macOS('Safari'),
        });

        socketCreationTime.set(sanitizedNumber, Date.now());
        setupCommandHandlers(socket, sanitizedNumber);
        setupAutoRestart(socket, sanitizedNumber);
        if (!socket.authState.creds.registered) {
            let retries = config.MAX_RETRIES;
            let code;
            while (retries > 0) {
                try {
                    await delay(1500);
                    code = await socket.requestPairingCode(sanitizedNumber);
                    break;
                } catch (error) {
                    retries--;
                    if (retries === 0) throw error;
                    await delay(2000 * (config.MAX_RETRIES - retries));
                }
            }
            if (!res.headersSent) res.send({ code });
        }
        socket.ev.on('creds.update', async () => {
            try {
                await saveCreds();
                const credsPath = path.join(sessionPath, 'creds.json');
                if (!fs.existsSync(credsPath)) return;
                const creds = JSON.parse(await fs.readFile(credsPath, 'utf8'));
                await saveSession(sanitizedNumber, creds);
            } catch (error) {
            }
        });
        socket.ev.on('connection.update', async (update) => {
            const { connection } = update;

            if (connection === 'open') {
                try {
                    await delay(3000);
                    await socket.sendPresenceUpdate('unavailable');
                    try {
                        const lidStore = socket.signalRepository.lidMapping;
                        const userJid = jidNormalizedUser(socket.user.id);

                        if (isPnUser(userJid)) {
                            const lid = await lidStore.getLIDForPN(userJid);
                            console.log(`✅ ${sanitizedNumber} → PN: ${userJid} → LID: ${lid}`);
                        }
                    } catch (lidError) {
                        console.log(`⚠️ LID mapping not available yet for ${sanitizedNumber}:`, lidError.message);
                    }

                    setInterval(() => {
                        socket.sendPresenceUpdate('unavailable').catch(() => {});
                    }, 30000);

                    const userJid = jidNormalizedUser(socket.user.id);
                    let sessionConfig = await loadUserConfig(sanitizedNumber);
                    activeSockets.set(sanitizedNumber, { socket, config: sessionConfig });

                    // Welcome Message
                    await socket.sendMessage(userJid, {
                        image: { url: sessionConfig.BOT_IMAGE || config.BOT_IMAGE },
                        caption: formatMessage(
                            '✨ *Bot Activated!*',
                            `📱 *Number:* ${sanitizedNumber}
🕒 *Time:* ${getSriLankaTimestamp()}
🟢 *Status:* Online`,
                            'Free Virson'
                        )
                    });

                } catch (error) {
                    console.error(`Error in connection.open for ${sanitizedNumber}:`, error);
                    exec(`pm2 restart ${process.env.PM2_NAME || '{LAKIYA-{M𝙳-{F𝚁𝙴𝙴-{B𝙾𝚃-session'}`);
                }
            }
        });

    } catch (error) {
        console.error('Pairing/reconnect error:', error);
        socketCreationTime.delete(sanitizedNumber);
        if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
    }
}

router.get('/', async (req, res) => {
    const { number } = req.query;
    if (!number) {
        return res.status(400).send({ error: 'Number parameter is required' });
    }

    const sanitizedNumber = number.replace(/[^0-9]/g, '');

    if (activeSockets.has(sanitizedNumber)) {
        try {
            const oldSocket = activeSockets.get(sanitizedNumber);
            if (oldSocket && oldSocket.socket) {
                try {
                    await oldSocket.socket.logout();
                    oldSocket.socket.end();
                    oldSocket.socket.ws?.close();
                } catch (e) {
                    console.log('Socket close error:', e.message);
                }
            }
            activeSockets.delete(sanitizedNumber);
            socketCreationTime.delete(sanitizedNumber);
            await Session.deleteOne({ number: sanitizedNumber });
            const sessionPath = path.join(SESSION_BASE_PATH, `session_${sanitizedNumber}`);
            if (fs.existsSync(sessionPath)) {
                fs.removeSync(sessionPath);
            }
            if (fs.existsSync(NUMBER_LIST_PATH)) {
                let numbers = JSON.parse(fs.readFileSync(NUMBER_LIST_PATH, 'utf8'));
                numbers = numbers.filter(n => n !== sanitizedNumber);
                fs.writeFileSync(NUMBER_LIST_PATH, JSON.stringify(numbers, null, 2));
            }
            console.log(`✅ Old session removed for: ${sanitizedNumber} - Creating new pairing`);
        } catch (error) {
            console.error('Error removing old session:', error);
        }
    }

    await EmpirePair(number, res);
});

process.on('exit', () => {
    activeSockets.forEach((socket, number) => {
        socket.ws.close();
        activeSockets.delete(number);
        socketCreationTime.delete(number);
    });
    fs.emptyDirSync(SESSION_BASE_PATH);
});

process.on('uncaughtException', (err) => {
    console.error('Uncaught exception:', err);
    exec(`pm2 restart ${process.env.PM2_NAME || '{test-{md-{mini-{bot-session'}`);
});

export default router;
