var fs = require("fs");
const steem = require('steem');
var utils = require('./utils');
var firebase = require('firebase-admin');
var firebaseServiceAccount = require('./firebase-credentials.json');

var account = null;
var sbd_balance = 0;
var steem_balance = 0;
var steem_power_balance = 0;
var steem_reserve_balance = 0;
var sbd_reserve_balance = 0;
var last_trans = 0;
var outstanding_bids = [];
var delegators = [];
var last_round = [];
var next_round = [];
var blacklist = [];
var whitelist = {};
var config = null;
var first_load = true;
var isVoting = false;
var last_withdrawal = null;
var use_delegators = false;
var round_end_timeout = -1;
var steem_price = 1;  // This will get overridden with actual prices if a price_feed_url is specified in settings
var sbd_price = 1;    // This will get overridden with actual prices if a price_feed_url is specified in settings
var version = '1.9.3';
var state = null;
var roi = 2;
var max_bid_sbd = 9999;
var min_bid_sbd = 0.001;


startup();

function loadFirebase() {
  firebase.initializeApp({
    credential: firebase.credential.cert(firebaseServiceAccount),
    databaseURL: 'https://steem-bid-bot.firebaseio.com/'
  });
  
  utils.log("Firebase started");
  
  var ref = firebase.database().ref(config.account);
  
  //Whitelist
  ref.child('whitelist').once('value').then(function(snapshot) {
    whitelist = snapshot.val();
    startEventsOnWhitelist();
  });
  
  //State
  ref.child('state').once('value').then(function(snapshot){
    state = snapshot.val();
    loadState();
  });
  
  ref.child('state/roi').on('value', function(data) {
    roi = data.val();
    utils.log("roi update: "+roi);
  });
  
  ref.child('max_bid_sbd').on('value', function(data){
    max_bid_sbd = data.val();
    utils.log("max bid update: "+max_bid_sbd+" sbd");
  });
  
  ref.child('min_bid_sbd').on('value', function(data){
    min_bid_sbd = data.val();
    utils.log("min bid update: "+min_bid_sbd+" sbd");
  });
  
  //Delegators
  use_delegators = config.auto_withdrawal && config.auto_withdrawal.active && config.auto_withdrawal.accounts.find(a => a.name == '$delegators');
  
  if(use_delegators) {
    ref.child('delegators').once('value').then(function(snapshot){
      if(snapshot.val() != null) delegators = snapshot.val();

      //var vests = delegators.reduce(function (total, v) { return total + parseFloat(v.vesting_shares); }, 0);
      var vests = 0;
      var length = 0;
      for(var d in delegators){
        if(delegators[d].vesting_shares){
          var vs = parseFloat(delegators[d].vesting_shares);
          if(vs >= 0){
            vests += vs;
            length++;
          }  
        }
      }  
      utils.log('Delegators Loaded (from firebase) - ' + length + ' delegators and ' + vests + ' VESTS in total!');
    });
  }
}  
  
function startEventsOnWhitelist(){
  var ref = firebase.database().ref(config.account+'/whitelist');
  
  ref.on('child_added', function(data) {
    whitelist[data.key] = data.val();    
  });
  
  ref.on('child_removed', function(data) {
    utils.log("account removed from whitelist: "+data.key);
    delete whitelist[data.key];    
  });
  
  firebase.database().ref(config.account+'/delegators').on('child_changed', function(data) {
    delegators[data.key] = data.val();
    utils.log("Delegator @"+data.key+" updated his preferences: sbd_reward_percentage: "+data.val().sbd_reward_percentage+", curation_reward_percentage: "+data.val().curation_reward_percentage);
  });
}

function startup() {
  // Load the settings from the config file
  loadConfig();
  
  // Connect to the specified RPC node
  var rpc_node = config.rpc_nodes ? config.rpc_nodes[0] : (config.rpc_node ? config.rpc_node : 'https://api.steemit.com');
  steem.api.setOptions({ transport: 'http', uri: rpc_node, url: rpc_node });

  utils.log("* START - Version: " + version + " *");
  utils.log("Connected to: " + rpc_node);

  if(config.backup_mode)
    utils.log('*** RUNNING IN BACKUP MODE ***');

  // Load Steem global variables
  utils.updateSteemVariables();

  // If the API is enabled, start the web server
  if(config.api && config.api.enabled) {
    var express = require('express');
    var app = express();
    var port = process.env.PORT || config.api.port

    app.use(function(req, res, next) {
      res.header("Access-Control-Allow-Origin", "*");
      res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
      next();
    });

    app.get('/api/bids', (req, res) => res.json({ current_round: outstanding_bids, last_round: last_round }));
    app.listen(port, () => utils.log('API running on port ' + port))
  }
  
  // Load data from firebase database
  loadFirebase();


  // Check whether or not auto-withdrawals are set to be paid to delegators.
  

  // If so then we need to load the list of delegators to the account
  
  // Schedule to run every 10 seconds
  setInterval(startProcess, 10000);

  // Load updated STEEM and SBD prices every 30 minutes
  loadPrices();
  setInterval(loadPrices, 30 * 60 * 1000);
}

function startProcess() {
  // Load the settings from the config file each time so we can pick up any changes
  loadConfig();

  // Load the bot account info
  steem.api.getAccounts([config.account], function (err, result) {
    if (result && !err) {
      account = result[0];

			if (account && !isVoting) {
				// Load the current voting power of the account
				var vp = utils.getVotingPower(account);

				if(config.detailed_logging) {
					var bids_steem = utils.format(outstanding_bids.reduce(function(t, b) { return t + ((b.currency == 'STEEM') ? b.amount : 0); }, 0), 3);
					var bids_sbd = utils.format(outstanding_bids.reduce(function(t, b) { return t + ((b.currency == 'SBD') ? b.amount : 0); }, 0), 3);
					utils.log((config.backup_mode ? '* BACKUP MODE *' : '') + 'Voting Power: ' + utils.format(vp / 100) + '% | Time until next round: ' + utils.toTimer(utils.timeTilFullPower(vp)) + ' | Bids: ' + outstanding_bids.length + ' | ' + bids_sbd + ' SBD | ' + bids_steem + ' STEEM');
				}

				// We are at 100% voting power - time to vote!
				if (vp >= 10000 && outstanding_bids.length > 0 && round_end_timeout < 0) {
					round_end_timeout = setTimeout(function() {
						round_end_timeout = -1;

						// Don't process any bids while we are voting due to race condition (they will be processed when voting is done).
						isVoting = first_load = true;

						// Make a copy of the list of outstanding bids and vote on them
						//startVoting(outstanding_bids.slice().reverse());
                        startVoting(outstanding_bids[0]);
                        
						//remove bid from outstanding bids
						//outstanding_bids.splice(0,1);

						// Reset the next round
						next_round = [];

						// Send out earnings if frequency is set to every round
						if (config.auto_withdrawal.frequency == 'round_end')
							processWithdrawals();

						// Save the state of the bot to disk
						//saveState();
					}, 30 * 1000);
				}

				// Load transactions to the bot account
				getTransactions();

				// Save the state of the bot to disk
				saveState();
                
                // Save account info
                saveAccount();               

				// Check if there are any rewards to claim.
				claimRewards();

				// Check if it is time to withdraw funds.
				if (config.auto_withdrawal.frequency == 'daily')
					checkAutoWithdraw();
			}
    } else
      logError('Error loading bot account: ' + err);
  });
}

function loadState(){  
  if (state.last_trans)
    last_trans = state.last_trans;

  if (state.outstanding_bids)
    outstanding_bids = state.outstanding_bids;

  if (state.last_round)
    last_round = state.last_round;

  if (state.next_round)
    next_round = state.next_round;
    
  if(state.last_withdrawal)
    last_withdrawal = state.last_withdrawal;
  
  if(state.roi)
    roi = state.roi;
    
  if(state.sbd_balance)
    sbd_balance = parseFloat(state.sbd_balance);
      
  if(state.steem_balance)
    steem_balance = parseFloat(state.steem_balance);
    
  if(state.steem_power_balance)
    steem_power_balance = parseFloat(state.steem_power_balance);

  if(state.steem_reserve_balance)
    steem_reserve_balance = parseFloat(state.steem_reserve_balance);
  
  if(state.sbd_reserve_balance)
    sbd_reserve_balance = parseFloat(state.sbd_reserve_balance);
    
  utils.log('Restored saved bot state: ' + JSON.stringify({ last_trans: last_trans, bids: outstanding_bids.length, last_withdrawal: last_withdrawal, sbd_balance: sbd_balance, steem_balance: steem_balance, steem_power_balance: steem_power_balance, steem_reserve_balance: steem_reserve_balance, sbd_reserve_balance: sbd_reserve_balance }));
}

function startVoting(bid) {
  if(config.backup_mode) {
    utils.log('*** Bidding Round End - Backup Mode, no voting ***');
    setTimeout(function () { isVoting = false; first_load = true; }, 5 * 60 * 1000);
    return;
  }

  var vote_value = utils.getVoteValue(100, account, 10000);
  var vote_value_usd = vote_value / 2 * sbd_price + vote_value / 2;
  
  bid.weight = Math.round(10000 * roi * getUsdValue(bid)/vote_value_usd);
  bid.weight = bid.weight > 10000 ? 10000 : bid.weight;

  sendComment(bid);
  sendVote(bid,0);
}

function sendVote(bid, retries, callback) {
  utils.log('Bid Weight: ' + bid.weight);
  steem.broadcast.vote(config.posting_key, account.name, bid.author, bid.permlink, bid.weight, function (err, result) {
    if (!err && result) {
      utils.log(utils.format(bid.weight / 100) + '% vote cast for: @' + bid.author + '/' + bid.permlink);

      isVoting = false;
      first_load = true;
      
      var id = outstanding_bids.findIndex(function(b){return b.author == bid.author && b.permlink == bid.permlink});
      if(id >= 0){
        outstanding_bids.splice(id,1);
        saveState();
      }else{
        utils.log("Error trying to remove bid from outstanding_bids: Not found");
      }  
      
      if (callback)
        callback();
    } else {
      logError('Error sending vote for: @' + bid.author + '/' + bid.permlink + ', Error: ' + err);

      // Try again on error
      if(retries < 2)
        setTimeout(function() { sendVote(bid, retries + 1, callback); }, 10000);
      else {
        utils.log('============= Vote transaction failed three times for: @' + bid.author + '/' + bid.permlink + ' Bid Amount: ' + bid.amount + ' ' + bid.currency + ' ===============');
        logFailedBid(bid, err);
        
        isVoting = false;
        first_load = true;

        if (callback)
          callback();
      }
    }
  });
}

function sendComment(bid) {
  var content = null;

  if(config.comment_location && config.comment_location != '') {
    content = fs.readFileSync(config.comment_location, "utf8");
  } else if (config.promotion_content && config.promotion_content != '') {
    content = config.promotion_content;
  }

  // If promotion content is specified in the config then use it to comment on the upvoted post
  if (content && content != '') {

    // Generate the comment permlink via steemit standard convention
    var permlink = 're-' + bid.author.replace(/\./g, '') + '-' + bid.permlink + '-' + new Date().toISOString().replace(/-|:|\./g, '').toLowerCase();

    // Replace variables in the promotion content
    content = content.replace(/\{weight\}/g, utils.format(bid.weight / 100)).replace(/\{botname\}/g, config.account).replace(/\{sender\}/g, bid.sender);

    // Broadcast the comment
    steem.broadcast.comment(config.posting_key, bid.author, bid.permlink, account.name, permlink, permlink, content, '{"app":"postpromoter/' + version + '"}', function (err, result) {
      if (!err && result) {
        utils.log('Posted comment: ' + permlink);
      } else {
        logError('Error posting comment: ' + permlink);
      }
    });
  }

  // Check if the bot should resteem this post
  if (config.min_resteem && bid.amount >= config.min_resteem)
    resteem(bid);
}

function resteem(bid) {
  var json = JSON.stringify(['reblog', {
    account: config.account,
    author: bid.author,
    permlink: bid.permlink
  }]);

  steem.broadcast.customJson(config.posting_key, [], [config.account], 'follow', json, (err, result) => {
    if (!err && result) {
      utils.log('Resteemed Post: @' + bid.sender + '/' + bid.permlink);
    } else {
      utils.log('Error resteeming post: @' + bid.sender + '/' + bid.permlink);
    }
  });
}

function getTransactions(callback) {
  var num_trans = 50;

  // If this is the first time the bot is ever being run, start with just the most recent transaction
  if (first_load && last_trans == 0) {
    utils.log('First run - starting with last transaction on account.');
    num_trans = 1;
  }

  // If this is the first time the bot is run after a restart get a larger list of transactions to make sure none are missed
  if (first_load && last_trans > 0) {
    utils.log('First run - loading all transactions since bot was stopped.');
    num_trans = 1000;
  }

  steem.api.getAccountHistory(account.name, -1, num_trans, function (err, result) {
    first_load = false;

    if (err || !result) {
      logError('Error loading account history: ' + err);

      if (callback)
        callback();

      return;
    }

    for (var i = 0; i < result.length; i++) {
      var trans = result[i];
      var op = trans[1].op;
      
        if(trans[0] > last_trans + 1) {
          utils.log('***** MISSED TRANSACTION(S) - last_trans: ' + last_trans + ', current_trans: ' + trans[0]);
        }

        // Check that this is a new transaction that we haven't processed already
        if(trans[0] > last_trans) {

          // We only care about transfers to the bot
          if (op[0] == 'transfer' && op[1].to == account.name) {
            var amount = parseFloat(op[1].amount);
            var currency = utils.getCurrency(op[1].amount);
            
            //search for liquid steem to reserve
            if(op[1].memo.substring(0,8).toLowerCase() == 'transfer'){
              if(currency == 'STEEM') steem_reserve_balance += amount;                
              if(currency == 'SBD') sbd_reserve_balance += amount;                
              addToDebt(op[1].from,amount,currency);
            }else{
              //Incoming Bid
            
              utils.log("Incoming Bid! From: " + op[1].from + ", Amount: " + op[1].amount + ", memo: " + op[1].memo);

              // Check for min and max bid values in configuration settings
              limitbids = getMinMaxBid(currency);
              var min_bid = limitbids.min;
              var max_bid = limitbids.max;

              if(config.disabled_mode) {
                // Bot is disabled, refund all Bids
                refund(op[1].from, amount, currency, 'bot_disabled');
              } else if(amount < min_bid) {
                // Bid amount is too low (make sure it's above the min_refund_amount setting)
                if(!config.min_refund_amount || amount >= config.min_refund_amount)
                  refund(op[1].from, amount, currency, 'below_min_bid');
                else {
                  utils.log('Invalid bid - below min bid amount and too small to refund.');
                }
              } else if (amount > max_bid) {
                // Bid amount is too high
                refund(op[1].from, amount, currency, 'above_max_bid');
              } else if(config.currencies_accepted && config.currencies_accepted.indexOf(currency) < 0) {
                // Sent an unsupported currency
                refund(op[1].from, amount, currency, 'invalid_currency');
              } else {
                // Bid amount is just right!
                checkPost(op[1].memo, amount, currency, op[1].from, 0);
              }
            }  
          } else if(use_delegators && op[0] == 'delegate_vesting_shares' && op[1].delegatee == account.name) {
            // If we are paying out to delegators, then update the list of delegators when new delegation transactions come in
            
            //var delegator = delegators.find(d => d.delegator == op[1].delegator);
            var d = dot2comma(op[1].delegator);
            var delegator = delegators[d];

            if(delegator){
              delegator.new_vesting_shares = op[1].vesting_shares;
              firebase.database().ref(config.account+'/delegators/'+d+'/new_vesting_shares').set(op[1].vesting_shares);
            }else {
			  delegator = { vesting_shares: 0, new_vesting_shares: op[1].vesting_shares, sbd_reward_percentage: 100, curation_reward_percentage: 100 };
              delegators[d] = delegator;
              
              firebase.database().ref(config.account+'/delegators/'+d).set(delegator);
			}

            // Save the updated list of delegators to disk
            //saveDelegators();

						// Check if we should send a delegation message
						if(parseFloat(delegator.new_vesting_shares) > parseFloat(delegator.vesting_shares) && config.transfer_memos['delegation'] && config.transfer_memos['delegation'] != '')
							refund(op[1].delegator, 0.001, 'SBD', 'delegation', 0, utils.vestsToSP(parseFloat(delegator.new_vesting_shares)).toFixed());

            utils.log('*** Delegation Update - ' + op[1].delegator + ' has delegated ' + op[1].vesting_shares);
          }

          // Save the ID of the last transaction that was processed.
          last_trans = trans[0];
        }
    }

    if (callback)
      callback();
  });
}

function checkRoundFillLimit(amount, currency) {
  if(config.round_fill_limit == null || config.round_fill_limit == undefined || isNaN(config.round_fill_limit))
    return false;

  var vote_value = utils.getVoteValue(100, account, 10000);
  var vote_value_usd = vote_value / 2 * sbd_price + vote_value / 2;
  var bid_value = outstanding_bids.reduce(function(t, b) { return t + b.amount * ((b.currency == 'SBD') ? sbd_price : steem_price) }, 0);
  var new_bid_value = amount * ((currency == 'SBD') ? sbd_price : steem_price);

  // Check if the value of the bids is over the round fill limit
  return (vote_value_usd * 0.75 * config.round_fill_limit < bid_value + new_bid_value);
}

function checkPost(memo, amount, currency, sender, retries) {
    // Parse the author and permlink from the memo URL
    var permLink = memo.substr(memo.lastIndexOf('/') + 1);
    var site = memo.substring(memo.indexOf('://')+3,memo.indexOf('/', memo.indexOf('://')+3));
    switch(site) {
      case 'd.tube':
          var author = memo.substring(memo.indexOf("/v/")+3,memo.lastIndexOf('/'));
          break;
      case 'dmania.lol':
          var author = memo.substring(memo.indexOf("/post/")+6,memo.lastIndexOf('/'));
          break;
      default:
          var author = memo.substring(memo.lastIndexOf('@') + 1, memo.lastIndexOf('/'));
    }

    if (author == '' || permLink == '') {
      refund(sender, amount, currency, 'invalid_post_url');
      return;
    }
    
    // Make sure the author isn't on the blacklist!
    if(searchAuthor(author, whitelist) == '' && (blacklist.indexOf(author) >= 0 || blacklist.indexOf(sender) >= 0))
    {
      handleBlacklist(author, sender, amount, currency);
      return;
    }
    
    // If this bot is whitelist-only then make sure the author is on the whitelist
    if(config.blacklist_settings.whitelist_only && searchAuthor(author, whitelist) == '') {
      refund(sender, amount, currency, 'whitelist_only');
      return;
    }
    
    var authorAux = author.replace(/[.]/g,",");
    if(whitelist[authorAux].last_bid){
      if(whitelist[authorAux].last_bid > (new Date()).getTime() - 1000*60*60*24){
        refund(sender, amount, currency, 'bids_per_day');
        return;
      }
    }else{
      utils.log("no last bid");
    }

    // Check if this author has gone over the max bids per author per round
    /*if(config.max_per_author_per_round && config.max_per_author_per_round > 0) {
      if(outstanding_bids.filter(b => b.author == author).length >= config.max_per_author_per_round)
      {
        refund(sender, amount, currency, 'bids_per_round');
        return;
      }
    }*/

    var push_to_next_round = false;

    steem.api.getContent(author, permLink, function (err, result) {
        if (!err && result && result.id > 0) {

            // If comments are not allowed then we need to first check if the post is a comment
            if(!config.allow_comments && (result.parent_author != null && result.parent_author != '')) {
              refund(sender, amount, currency, 'no_comments');
              return;
            }

            // Check if any tags on this post are blacklisted in the settings
            if (config.blacklist_settings.blacklisted_tags && config.blacklist_settings.blacklisted_tags.length > 0 && result.json_metadata && result.json_metadata != '') {
              var tags = JSON.parse(result.json_metadata).tags;

              if (tags && tags.length > 0) {
                var tag = tags.find(t => config.blacklist_settings.blacklisted_tags.indexOf(t) >= 0);

                if(tag) {
                  refund(sender, amount, currency, 'blacklist_tag', 0, tag);
                  return;
                }
              }
            }

            var created = new Date(result.created + 'Z');
            var time_until_vote = utils.timeTilFullPower(utils.getVotingPower(account));

            // Get the list of votes on this post to make sure the bot didn't already vote on it (you'd be surprised how often people double-submit!)
            var votes = result.active_votes.filter(function(vote) { return vote.voter == account.name; });

            if (votes.length > 0 || (new Date() - created) >= (config.max_post_age * 60 * 60 * 1000)) {
                // This post is already voted on by this bot or the post is too old to be voted on
                refund(sender, amount, currency, ((votes.length > 0) ? 'already_voted' : 'max_age'));
                return;
            }

            // Check if this post has been flagged by any flag signal accounts
            if(config.blacklist_settings.flag_signal_accounts) {
              var flags = result.active_votes.filter(function(v) { return v.percent < 0 && config.blacklist_settings.flag_signal_accounts.indexOf(v.voter) >= 0; });

              if(flags.length > 0) {
                handleFlag(sender, amount, currency);
                return;
              }
            }

            // Check if this post is below the minimum post age
            /*if(config.min_post_age && config.min_post_age > 0 && (new Date() - created + (time_until_vote * 1000)) < (config.min_post_age * 60 * 1000)) {
              push_to_next_round = true;
              refund(sender, 0.001, currency, 'min_age');
            }*/
        } else if(result && result.id == 0) {
          // Invalid memo
          refund(sender, amount, currency, 'invalid_post_url');
          return;
        } else {
          logError('Error loading post: ' + memo + ', Error: ' + err);

          // Try again on error
          if(retries < 2){
            setTimeout(function() { checkPost(memo, amount, currency, sender, retries + 1); }, 3000);
            return;
          }else {
            utils.log('============= Load post failed three times for: ' + memo + ' ===============');

            refund(sender, amount, currency, 'invalid_post_url');
            return;
          }
        }

        /*if(!push_to_next_round && checkRoundFillLimit(amount, currency)) {
          push_to_next_round = true;
          refund(sender, 0.001, currency, 'round_full');
        }*/

        // Add the bid to the current round or the next round if the current one is full or the post is too new
        //var round = push_to_next_round ? next_round : outstanding_bids;
        var round = outstanding_bids;

        // Check if there is already a bid for this post in the current round
        var existing_bid = round.find(bid => bid.url == result.url);

        if(existing_bid) {
          // There is already a bid for this post in the current round
          utils.log('Existing Bid Found - New Amount: ' + amount + ', Total Amount: ' + (existing_bid.amount + amount));

          var new_amount = 0;

          if(existing_bid.currency == currency) {
            new_amount = existing_bid.amount + amount;
          } else if(existing_bid.currency == 'STEEM') {
            new_amount = existing_bid.amount + amount * sbd_price / steem_price;
          } else if(existing_bid.currency == 'SBD') {
            new_amount = existing_bid.amount + amount * steem_price / sbd_price;
          }
          
          limitbids = getMinMaxBid(existing_bid.currency);
          var max_bid = limitbids.max;

          // Check that the new total doesn't exceed the max bid amount per post
          if (new_amount > max_bid)
            refund(sender, amount, currency, 'above_max_bid');
          else
            existing_bid.amount = new_amount;
        } else {
          // All good - push to the array of valid bids for this round
          utils.log('Valid Bid - Amount: ' + amount + ' ' + currency + ', Title: ' + result.title);
          if(currency == 'SBD') sbd_balance += amount;
          if(currency == 'STEEM') steem_balance += amount;
          round.push({ amount: amount, currency: currency, sender: sender, author: result.author, permlink: result.permlink, url: result.url, title: result.title });
          var author = result.author.replace(/[.]/g,",");
          firebase.database().ref(config.account+'/whitelist/'+author+'/last_bid').set((new Date()).getTime());  
        }

        // If a witness_vote transfer memo is set, check if the sender votes for the bot owner as witness and send them a message if not
        if (config.transfer_memos['witness_vote'] && config.transfer_memos['witness_vote'] != '') {
          checkWitnessVote(sender, sender, currency);
        } else if(!push_to_next_round && config.transfer_memos['bid_confirmation'] && config.transfer_memos['bid_confirmation'] != '') {
					// Send bid confirmation transfer memo if one is specified
					refund(sender, 0.001, currency, 'bid_confirmation', 0);
				}
    });
}

function handleBlacklist(author, sender, amount, currency) {
  utils.log('Invalid Bid - @' + author + ' is on the blacklist!');

  // Refund the bid only if blacklist_refunds are enabled in config
  if (config.blacklist_settings.refund_blacklist)
    refund(sender, amount, currency, 'blacklist_refund', 0);
  else {
    // Otherwise just send a 0.001 transaction with blacklist memo
    if (config.transfer_memos['blacklist_no_refund'] && config.transfer_memos['blacklist_no_refund'] != '')
      refund(sender, 0.001, currency, 'blacklist_no_refund', 0);

    // If a blacklist donation account is specified then send funds from blacklisted users there
    if (config.blacklist_settings.blacklist_donation_account)
      refund(config.blacklist_settings.blacklist_donation_account, amount - 0.001, currency, 'blacklist_donation', 0);
  }
}

function handleFlag(sender, amount, currency) {
  utils.log('Invalid Bid - This post has been flagged by one or more spam / abuse indicator accounts.');

  // Refund the bid only if blacklist_refunds are enabled in config
  if (config.blacklist_settings.refund_blacklist)
    refund(sender, amount, currency, 'flag_refund', 0);
  else {
    // Otherwise just send a 0.001 transaction with blacklist memo
    if (config.transfer_memos['flag_no_refund'] && config.transfer_memos['flag_no_refund'] != '')
      refund(sender, 0.001, currency, 'flag_no_refund', 0);

    // If a blacklist donation account is specified then send funds from blacklisted users there
    if (config.blacklist_settings.blacklist_donation_account)
      refund(config.blacklist_settings.blacklist_donation_account, amount - 0.001, currency, 'blacklist_donation', 0);
  }
}

function checkWitnessVote(sender, voter, currency) {
  if(!config.owner_account || config.owner_account == '')
    return;

  steem.api.getAccounts([voter], function (err, result) {
    if (result && !err) {
      if (result[0].proxy && result[0].proxy != '') {
        checkWitnessVote(sender, result[0].proxy, currency);
        return;
      }

      if(result[0].witness_votes.indexOf(config.owner_account) < 0)
        refund(sender, 0.001, currency, 'witness_vote', 0);
		  else if(config.transfer_memos['bid_confirmation'] && config.transfer_memos['bid_confirmation'] != '') {
				// Send bid confirmation transfer memo if one is specified
				refund(sender, 0.001, currency, 'bid_confirmation', 0);
			}
    } else
      logError('Error loading sender account to check witness vote: ' + err);
  });
}

function saveState() {
  var state = {
    outstanding_bids: outstanding_bids,
    last_round: last_round,
    next_round: next_round,
    last_trans: last_trans,
    last_withdrawal: last_withdrawal,
    sbd_balance: sbd_balance.toFixed(3),
    steem_balance: steem_balance.toFixed(3),
    steem_power_balance: steem_power_balance.toFixed(3),
    steem_reserve_balance: steem_reserve_balance.toFixed(3),
    sbd_reserve_balance: sbd_reserve_balance.toFixed(3),
    roi: roi,
    version: version
  };

  // Save the state of the bot to firebase
  firebase.database().ref(config.account+'/state').set(state);  
}

function saveAccount(){
  firebase.database().ref(config.account+'/account').set(account);
}

/*
function updateVersion(old_version, new_version) {
  utils.log('**** Performing Update Steps from version: ' + old_version + ' to version: ' + new_version);

  if(!old_version) {
    if(fs.existsSync('delegators.json')) {
      fs.rename('delegators.json', 'old-delegators.json', (err) => {
        if (err)
          utils.log('Error renaming delegators file: ' + err);
        else
          utils.log('Renamed delegators.json file so it will be reloaded from account history.');
      });
    }
  }
}*/

/*function saveDelegators() {
  // Save the list of delegators to firebase
  firebase.database().ref(config.account+'/delegators').set(delegators);    
}*/

function refund(sender, amount, currency, reason, retries, data) {
  if(config.backup_mode) {
    utils.log('Backup Mode - not sending refund of ' + amount + ' ' + currency + ' to @' + sender + ' for reason: ' + reason);
    return;
  }

  if(!retries)
    retries = 0;

  // Make sure refunds are enabled and the sender isn't on the no-refund list (for exchanges and things like that).
  if (reason != 'forward_payment' && (!config.refunds_enabled || sender == config.account || (config.no_refund && config.no_refund.indexOf(sender) >= 0))) {
    utils.log("Invalid bid - " + reason + ' NO REFUND');

    // If this is a payment from an account on the no_refund list, forward the payment to the post_rewards_withdrawal_account
    if(config.no_refund && config.no_refund.indexOf(sender) >= 0 && config.post_rewards_withdrawal_account && config.post_rewards_withdrawal_account != '' && sender != config.post_rewards_withdrawal_account)
      refund(config.post_rewards_withdrawal_account, amount, currency, 'forward_payment', 0, sender);

    return;
  }
  
  limitbids = getMinMaxBid(currency);
  
  // Replace variables in the memo text
  var memo = config.transfer_memos[reason];
  memo = memo.replace(/{amount}/g, utils.format(amount, 3) + ' ' + currency);
  memo = memo.replace(/{currency}/g, currency);
  memo = memo.replace(/{min_bid}/g, (limitbids.min.toFixed(3)+' '+currency));
  memo = memo.replace(/{max_bid}/g, (limitbids.max.toFixed(3)+' '+currency));
  memo = memo.replace(/{account}/g, config.account);
  memo = memo.replace(/{owner}/g, config.owner_account);
  memo = memo.replace(/{min_age}/g, config.min_post_age);
  memo = memo.replace(/{sender}/g, sender);
  memo = memo.replace(/{tag}/g, data);

  var days = Math.floor(config.max_post_age / 24);
  var hours = (config.max_post_age % 24);
  memo = memo.replace(/{max_age}/g, days + ' Day(s)' + ((hours > 0) ? ' ' + hours + ' Hour(s)' : ''));

  // Issue the refund.
  steem.broadcast.transfer(config.active_key, config.account, sender, utils.format(amount, 3) + ' ' + currency, memo, function (err, response) {
    if (err) {
      logError('Error sending refund to @' + sender + ' for: ' + amount + ' ' + currency + ', Error: ' + err);

      // Try again on error
      if(retries < 2)
        setTimeout(function() { refund(sender, amount, currency, reason, retries + 1, data) }, (Math.floor(Math.random() * 10) + 3) * 1000);
      else
        utils.log('============= Refund failed three times for: @' + sender + ' ===============');
    } else {
      utils.log('Refund of ' + amount + ' ' + currency + ' sent to @' + sender + ' for reason: ' + reason);
    }
  });
}

function claimRewards() {
  if (!config.auto_claim_rewards || config.backup_mode)
    return;

  // Make api call only if you have actual reward
  if (parseFloat(account.reward_steem_balance) > 0 || parseFloat(account.reward_sbd_balance) > 0 || parseFloat(account.reward_vesting_balance) > 0) {
    steem.broadcast.claimRewardBalance(config.posting_key, config.account, account.reward_steem_balance, account.reward_sbd_balance, account.reward_vesting_balance, function (err, result) {
      if (err) {
        utils.log('Error claiming rewards...will try again next time.');
        return;
      }
      
      sbd_balance += parseFloat(account.reward_sbd_balance);
      steem_balance += parseFloat(account.reward_steem_balance);
      steem_power_balance += utils.vestsToSP(parseFloat(account.reward_vesting_balance));
      
      var d = dot2comma(config.account);
      delegators[d] = {
        curation_reward_percentage: 100,
        sbd_reward_percentage: 100,
        vesting_shares: (parseFloat(account.vesting_shares) + parseFloat(account.reward_vesting_balance)) + " VESTS",
      };
      
      firebase.database().ref(config.account+'/delegators/'+d).set(delegators[d]);
      
      if(config.detailed_logging) {
        var rewards_message = "$$$ ==> Rewards Claim";
        if (parseFloat(account.reward_sbd_balance) > 0) { rewards_message = rewards_message + ' SBD: ' + parseFloat(account.reward_sbd_balance); }
        if (parseFloat(account.reward_steem_balance) > 0) { rewards_message = rewards_message + ' STEEM: ' + parseFloat(account.reward_steem_balance); }
        if (parseFloat(account.reward_vesting_balance) > 0) { rewards_message = rewards_message + ' VESTS: ' + parseFloat(account.reward_vesting_balance); }

        utils.log(rewards_message);      
      }
    });
  }
}

function checkAutoWithdraw() {
  // Check if auto-withdraw is active
  if (!config.auto_withdrawal.active)
    return;

  // If it's past the withdrawal time and we haven't made a withdrawal today, then process the withdrawal
  if (new Date(new Date().toDateString()) > new Date(last_withdrawal) && new Date().getHours() >= config.auto_withdrawal.execute_time) {
    processWithdrawals();
  }
}

function processWithdrawals() {
  if(config.backup_mode)
    return;

  var liquid_steem_power = steem_reserve_balance >= steem_power_balance ? steem_power_balance : steem_reserve_balance;
  var has_sbd = config.currencies_accepted.indexOf('SBD') >= 0 && sbd_balance > 0;
  var has_steem = config.currencies_accepted.indexOf('STEEM') >= 0 && steem_balance > 0;
  var has_steem_power = liquid_steem_power > 0; 
  utils.log("Withdrawals. sbd_balance="+sbd_balance+"  steem_balance="+steem_balance+"  liquid_steem_power="+liquid_steem_power);

  var sbd_bal = parseFloat(account.sbd_balance);
  var sbd_bal = sbd_balance >= sbd_bal ? sbd_bal : sbd_balance;
  var steem_bal = parseFloat(account.balance) - liquid_steem_power;
  var steem_bal = steem_bal < 0 ? 0 : steem_bal;
  var steem_bal = steem_balance >= steem_bal ? steem_bal : steem_balance;
  
  utils.log("Withdrawals. sbd_bal="+sbd_bal+"  steem_bal="+steem_bal+"  liquid_steem_power="+liquid_steem_power);

  if (has_sbd || has_steem || has_steem_power) {

    // Save the date of the last withdrawal
    last_withdrawal = new Date().toDateString();

    var total_stake = config.auto_withdrawal.accounts.reduce(function (total, info) { return total + info.stake; }, 0);

    var withdrawals = [];

    for(var i = 0; i < config.auto_withdrawal.accounts.length; i++) {
      var withdrawal_account = config.auto_withdrawal.accounts[i];
        
        // Get the total amount delegated by all delegators
        //var total_vests = delegators.reduce(function (total, v) { return total + parseFloat(v.vesting_shares); }, 0);
        var total_vests = 0;
        for(var d in delegators){
          if(delegators[d].vesting_shares){
            var vs = parseFloat(delegators[d].vesting_shares);
            if(vs >= 0) total_vests += vs;
          }
        }

        // Send the withdrawal to each delegator based on their delegation amount
        //for(var j = 0; j < delegators.length; j++) {
        for(var d in delegators){
          var delegator = delegators[d];
          var to_account = comma2dot(d);
          if(delegator.send_to) to_account = comma2dot(delegator.send_to);

          if(has_sbd) {
            // Check if there is already an SBD withdrawal to this account
            var withdrawal = withdrawals.find(w => w.to == to_account && w.currency == 'SBD');
            var perc_sbd = parseFloat(delegator.sbd_reward_percentage) / 100;
            perc_sbd = perc_sbd < 0 ? 0 : (perc_sbd>1 ? 1 : perc_sbd);
            var amountSBD = sbd_bal * (withdrawal_account.stake / total_stake) * (parseFloat(delegator.vesting_shares) / total_vests) - 0.001;
            var paymentSBD = perc_sbd * amountSBD;
            var donationSBD = amountSBD - paymentSBD;
            paymentSBD = paymentSBD > 0 ? paymentSBD : 0;
            donationSBD = donationSBD > 0 ? donationSBD : 0;

            if(withdrawal) {
              withdrawal.amount += paymentSBD;
            } else {
              withdrawals.push({
                to: to_account,
                currency: 'SBD',
                amount: paymentSBD,
                donation: donationSBD,
                delegator: d
              });
            }
          }

          if(has_steem || has_steem_power) {
            // Check if there is already a STEEM withdrawal to this account
            var withdrawal = withdrawals.find(w => w.to == to_account && w.currency == 'STEEM');
            var perc_steem = parseFloat(delegator.sbd_reward_percentage) / 100;
            var perc_sp = parseFloat(delegator.curation_reward_percentage) / 100;
            perc_steem = perc_steem < 0 ? 0 : (perc_steem>1 ? 1 : perc_steem);            
            perc_sp = perc_sp < 0 ? 0 : (perc_sp>1 ? 1 : perc_sp);
            
            var amountSteem = steem_bal * (withdrawal_account.stake / total_stake) * (parseFloat(delegator.vesting_shares) / total_vests) - 0.001;
            var amountSP = liquid_steem_power * (withdrawal_account.stake / total_stake) * (parseFloat(delegator.vesting_shares) / total_vests) - 0.001;
            
            var paymentSteem = perc_steem * amountSteem;
            var paymentSP = perc_sp * amountSP;
            var donationSteem = amountSteem - paymentSteem;
            var donationSP = amountSP - paymentSP;
            
            paymentSteem = paymentSteem > 0 ? paymentSteem : 0;
            paymentSP = paymentSP > 0 ? paymentSP : 0;
            donationSteem = donationSteem > 0 ? donationSteem : 0;
            donationSP = donationSP > 0 ? donationSP : 0;            

            if(withdrawal) {
              withdrawal.amount += paymentSteem;
              withdrawal.amountSP += paymentSP;
            } else {
              withdrawals.push({
                to: to_account,
                currency: 'STEEM',
                amount: paymentSteem,
                amountSP: paymentSP,
                donation: donationSteem,
                donationSP: donationSP,
                delegator: d
              });
            }
          }
        }      
    }

    // Check if the memo should be encrypted
    var encrypt = (config.auto_withdrawal.memo.startsWith('#') && config.memo_key && config.memo_key != '');

    if(encrypt) {
      // Get list of unique withdrawal account names
      var account_names = withdrawals.map(w => w.to).filter((v, i, s) => s.indexOf(v) === i);

      // Load account info to get memo keys for encryption
      steem.api.getAccounts(account_names, function (err, result) {
        if (result && !err) {
          for(var i = 0; i < result.length; i++) {
            var withdrawal_account = result[i];
            var matches = withdrawals.filter(w => w.to == withdrawal_account.name);

            for(var j = 0; j < matches.length; j++) {
              matches[j].memo_key = withdrawal_account.memo_key;
            }
          }

          sendWithdrawals(withdrawals);
        } else
          logError('Error loading withdrawal accounts: ' + err);
      });
    } else
      sendWithdrawals(withdrawals);
  }

  updateDelegations();
}

function updateDelegations() {
  for(var d in delegators){
    var delegator = delegators[d]; 
    if(parseFloat(delegator.new_vesting_shares) >= 0){
      delegator.vesting_shares = delegator.new_vesting_shares;
      delegator.new_vesting_shares = null;
      firebase.database().ref(config.account+'/delegators/'+d).set(delegator);
    }
  }  
}

function sendWithdrawals(withdrawals) {
  // Send out withdrawal transactions one at a time
  sendWithdrawal(withdrawals.pop(), 0, function() {
    // If there are more withdrawals, send the next one.
    if (withdrawals.length > 0)
      sendWithdrawals(withdrawals);
    else
      utils.log('========== Withdrawals Complete! ==========');
  });
}

function sendWithdrawal(withdrawal, retries, callback) {
  if(parseFloat(utils.format(withdrawal.amount, 3)) <= 0) {
    if(!withdrawal.amountSP || parseFloat(utils.format(withdrawal.amountSP, 3)) <= 0){
      if(callback)
        callback();

      return;
    }  
  }
  
  var amount = withdrawal.amount;
  if(withdrawal.amountSP) amount += withdrawal.amountSP;

  var formatted_amount = utils.format(amount, 3).replace(/,/g, '') + ' ' + withdrawal.currency;
  var memo = config.auto_withdrawal.memo.replace(/\{balance\}/g, formatted_amount);

  // Encrypt memo
  if (memo.startsWith('#') && config.memo_key && config.memo_key != '')
    memo = steem.memo.encode(config.memo_key, withdrawal.memo_key, memo);

  // Send the withdrawal amount to the specified account
  steem.broadcast.transfer(config.active_key, config.account, withdrawal.to, formatted_amount, memo, function (err, response) {
    if (err) {
      logError('Error sending withdrawal transaction to: ' + withdrawal.to + ', Error: ' + err);

      // Try again once if there is an error
      if(retries < 1)
        setTimeout(function() { sendWithdrawal(withdrawal, retries + 1, callback); }, 3000);
      else {
        utils.log('============= Withdrawal failed two times to: ' + withdrawal.to + ' for: ' + formatted_amount + ' ===============');

        if(callback)
          callback();
      }
    } else {    
      utils.log('$$$ Auto withdrawal: ' + formatted_amount + ' sent to @' + withdrawal.to);
      
      var d = withdrawal.delegator;

      if(withdrawal.currency == 'SBD'){
        sbd_balance -= withdrawal.amount;
        if(delegators[d].donation_sbd)
          delegators[d].donation_sbd += withdrawal.donation;
        else
          delegators[d].donation_sbd = withdrawal.donation;
      }
      
      if(withdrawal.currency == 'STEEM'){
        steem_balance -= withdrawal.amount;
        steem_power_balance -= withdrawal.amountSP;
        steem_reserve_balance -= withdrawal.amountSP;
        
        if(delegators[d].donation_steem)
          delegators[d].donation_steem += withdrawal.donation;
        else
          delegators[d].donation_steem = withdrawal.donation;
          
        if(delegators[d].donation_sp)  
          delegators[d].donation_sp += withdrawal.donationSP;
        else
          delegators[d].donation_sp = withdrawal.donationSP;
      }
      
      firebase.database().ref(config.account+'/delegators/'+d).set(delegators[d]);
      
      if(callback)
        callback();
    }
  });
}

function loadPrices() {
  // Require the "request" library for making HTTP requests
  var request = require("request");

  // Load the price feed data
  request.get('https://api.coinmarketcap.com/v1/ticker/steem/', function (e, r, data) {
    try {
      steem_price = parseFloat(JSON.parse(data)[0].price_usd);

      utils.log("Loaded STEEM price: " + steem_price);
    } catch (err) {
      utils.log('Error loading STEEM price: ' + err);
    }
  });

  // Load the price feed data
  request.get('https://api.coinmarketcap.com/v1/ticker/steem-dollars/', function (e, r, data) {
    try {
      sbd_price = parseFloat(JSON.parse(data)[0].price_usd);

      utils.log("Loaded SBD price: " + sbd_price);
    } catch (err) {
      utils.log('Error loading SBD price: ' + err);
    }
  });
}

function getUsdValue(bid) { return bid.amount * ((bid.currency == 'SBD') ? sbd_price : steem_price); }
function getSteemSBDValue(usd,currency){ return usd / ((currency == 'SBD') ? sbd_price : steem_price); }

function getMinMaxBid(currency){
  var min_bid;
  var max_bid;
  if(currency == 'SBD'){
    min_bid = min_bid_sbd;
    max_bid = max_bid_sbd;
  }else{
    min_bid = min_bid_sbd * sbd_price / steem_price;
    max_bid = max_bid_sbd * sbd_price / steem_price;    
  }
  return {min: min_bid, max:max_bid};
}

function logFailedBid(bid, message) {
  if (message.indexOf('assert_exception') >= 0 && message.indexOf('ERR_ASSERTION') >= 0)
    return;

  var failed_bids = [];

  if(fs.existsSync("failed-bids.json"))
    failed_bids = JSON.parse(fs.readFileSync("failed-bids.json"));

  bid.error = message;
  failed_bids.push(bid);

  fs.writeFile('failed-bids.json', JSON.stringify(failed_bids), function (err) {
    if (err)
      utils.log('Error saving failed bids to disk: ' + err);
  });
}

function loadConfig() {
  config = JSON.parse(fs.readFileSync("config.json"));

  // Backwards compatibility for blacklist settings
  if(!config.blacklist_settings) {
    config.blacklist_settings = {
      flag_signal_accounts: config.flag_signal_accounts,
      blacklist_location: config.blacklist_location ? config.blacklist_location : 'blacklist',
      refund_blacklist: config.refund_blacklist,
      blacklist_donation_account: config.blacklist_donation_account,
      blacklisted_tags: config.blacklisted_tags
    };
  }

  var newBlacklist = [];

  // Load the blacklist
  utils.loadUserList(config.blacklist_settings.blacklist_location, function(list1) {
    var list = [];

    if(list1)
      list = list1;

    // Load the shared blacklist
    utils.loadUserList(config.blacklist_settings.shared_blacklist_location, function(list2) {
      if(list2)
        list = list.concat(list2.filter(i => list.indexOf(i) < 0));

      if(list1 || list2)
        blacklist = list;
    });
  });
}

function failover() {
  if(config.rpc_nodes && config.rpc_nodes.length > 1) {
    // Give it a minute after the failover to account for more errors coming in from the original node
    setTimeout(function() { error_count = 0; }, 60 * 1000);
  
    var cur_node_index = config.rpc_nodes.indexOf(steem.api.options.url) + 1;

    if(cur_node_index == config.rpc_nodes.length)
      cur_node_index = 0;

    var rpc_node = config.rpc_nodes[cur_node_index];

    steem.api.setOptions({ transport: 'http', uri: rpc_node, url: rpc_node });
    utils.log('');
    utils.log('***********************************************');
    utils.log('Failing over to: ' + rpc_node);
    utils.log('***********************************************');
    utils.log('');
  }
}

var error_count = 0;
function logError(message) {
  // Don't count assert exceptions for node failover
  if (message.indexOf('assert_exception') < 0 && message.indexOf('ERR_ASSERTION') < 0)
    error_count++;

  utils.log('Error Count: ' + error_count + ', Current node: ' + steem.api.options.url);
  utils.log(message);
}

// Check if 10+ errors have happened in a 3-minute period and fail over to next rpc node
function checkErrors() {
  if(error_count >= 10)
    failover();

  // Reset the error counter
  error_count = 0;
}
setInterval(checkErrors, 3 * 60 * 1000);

function searchAuthor(author, list){
  for(var key in list) {
    //if(list[key] == author) return key;
    acc = key.replace(/[,]/g,".");
    if(acc == author) return author;
  }
  return '';
}

function comma2dot(name){
  return name.replace(/[,]/g,".");
}

function dot2comma(name){
  return name.replace(/[.]/g,",");
}

function addToDebt(user,newAmount,currency){
  var ref = firebase.database().ref(config.account+'/debt/'+user+'/'+currency.toLowerCase());
  ref.once('value').then(function(snapshot){
    var amount = snapshot.val() > 0 ? snapshot.val() : 0;
    ref.set(amount + newAmount);
  });  
  utils.log("Transfer to reserve from @"+user+": "+newAmount+" "+currency);
}
